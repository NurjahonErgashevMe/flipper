"""
SheetsManager - Generic Google Sheets API wrapper

Данный модуль абстрагирован от любых специфических моделей данных.
Используется любым парсером для чтения URLs и записи результатов.
"""

import os
import logging
import time
from typing import List, Any, Dict, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Верхняя граница строк при чтении FILTERS через gridData (обычно десятки ссылок)
_SHEETS_URL_COLUMN_MAX_ROWS = 5000


def _extract_url_from_hyperlink_formula(formula: str) -> Optional[str]:
    """
    Достаёт первый аргумент из =HYPERLINK("url", "подпись") или с «;» (локаль Sheets).
    """
    if not formula:
        return None
    s = formula.strip()
    if len(s) < 12 or not s.upper().startswith("=HYPERLINK"):
        return None
    idx = s.upper().find("HYPERLINK")
    idx = s.find("(", idx)
    if idx < 0:
        return None
    i = idx + 1
    n = len(s)
    while i < n and s[i].isspace():
        i += 1
    if i >= n:
        return None
    quote = s[i]
    if quote not in '"\'':
        return None
    i += 1
    parts: List[str] = []
    while i < n:
        c = s[i]
        if c == quote:
            if quote == '"' and i + 1 < n and s[i + 1] == '"':
                parts.append('"')
                i += 2
                continue
            if quote == "'" and i + 1 < n and s[i + 1] == "'":
                parts.append("'")
                i += 2
                continue
            break
        parts.append(c)
        i += 1
    out = "".join(parts).strip()
    return out if out.startswith("http") else None


def _url_from_text_format_runs(cell: Dict[str, Any]) -> Optional[str]:
    for run in cell.get("textFormatRuns") or []:
        fmt = run.get("format") or {}
        link = fmt.get("link") or {}
        uri = link.get("uri")
        if uri and str(uri).strip().startswith("http"):
            return str(uri).strip()
    return None


def _url_from_grid_cell(cell: Optional[Dict[str, Any]]) -> Optional[str]:
    """Реальный URL из ячейки: hyperlink, rich-text link, формула HYPERLINK, текст."""
    if not cell:
        return None
    h = cell.get("hyperlink")
    if h and str(h).strip().startswith("http"):
        return str(h).strip()
    u = _url_from_text_format_runs(cell)
    if u:
        return u
    ue = cell.get("userEnteredValue") or {}
    fv = ue.get("formulaValue")
    if fv:
        extracted = _extract_url_from_hyperlink_formula(fv)
        if extracted:
            return extracted
    for key in ("stringValue", "numberValue", "boolValue"):
        if key not in ue:
            continue
        raw = ue.get(key)
        if raw is None:
            continue
        t = str(raw).strip()
        if t.startswith("http"):
            return t
        break
    disp = cell.get("formattedValue")
    if disp:
        t = str(disp).strip()
        if t.startswith("http"):
            return t
    return None


class SheetsManager:
    """
    Управляет Google Sheets документом.
    
    API:
    - get_urls(tab_name: str, column: str) -> List[str]
    - write_row(tab_name: str, row: List[Any]) -> bool
    - write_rows(tab_name: str, rows: List[List[Any]]) -> bool
    - read_range(range_str: str) -> List[List[Any]]
    """

    def __init__(self, spreadsheet_id: str = None, credentials_path: str = None):
        """
        Args:
            spreadsheet_id: ID документа Google Sheets (если None, берется из SPREADSHEET_ID env)
            credentials_path: Путь к JSON ключу Service Account (если None, берется из CREDENTIALS_PATH env или /app/credentials.json)
        """
        # Получаем spreadsheet_id из env если не передан
        if spreadsheet_id is None:
            spreadsheet_id = os.getenv("SPREADSHEET_ID")
            if not spreadsheet_id:
                raise ValueError("SPREADSHEET_ID must be provided or set in environment variables")
        
        # Получаем credentials_path из env если не передан
        if credentials_path is None:
            credentials_path = os.getenv("CREDENTIALS_PATH", "/app/credentials.json")
        
        self.spreadsheet_id = spreadsheet_id
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Credentials file not found at {credentials_path}. "
                f"Please ensure you have Google Service Account JSON key."
            )

        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        # Отключаем file_cache чтобы избежать warnings
        self.service = build("sheets", "v4", credentials=self.credentials, cache_discovery=False)
        self.sheet = self.service.spreadsheets()
        # Квота Google: ~60 read/min/user — не чаще одного чтения раз в READ_SPACING_SEC
        self._last_read_mono: float = 0.0
        self._read_spacing_sec: float = float(os.getenv("SHEETS_READ_SPACING_SEC", "1.05"))
        self._sheet_id_cache: Dict[str, int] = {}

        logger.info(f"SheetsManager initialized for spreadsheet {spreadsheet_id}")

    def _throttle_read(self) -> None:
        gap = self._read_spacing_sec
        now = time.monotonic()
        wait = gap - (now - self._last_read_mono)
        if wait > 0:
            time.sleep(wait)
        self._last_read_mono = time.monotonic()

    def _execute_with_retry(self, fn, max_retries: int = 14):
        """Повтор при 429 (Read requests per minute per user)."""
        last_err = None
        for attempt in range(max_retries):
            try:
                return fn()
            except HttpError as e:
                last_err = e
                status = int(e.resp.status) if e.resp is not None else 0
                if status == 429 and attempt < max_retries - 1:
                    sleep_s = min(120, 8 + 6 * attempt)
                    logger.warning(
                        "Google Sheets 429 (quota), sleep %ss (attempt %s/%s)",
                        sleep_s,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(sleep_s)
                    continue
                raise
        if last_err:
            raise last_err

    def batch_get_value_ranges(self, ranges: List[str]) -> Dict[str, List[List[Any]]]:
        """Один batchGet на несколько диапазонов (меньше запросов, чем по отдельности)."""
        if not ranges:
            return {}
        self._throttle_read()

        def call():
            return (
                self.sheet.values()
                .batchGet(spreadsheetId=self.spreadsheet_id, ranges=ranges)
                .execute()
            )

        result = self._execute_with_retry(call)
        out: Dict[str, List[List[Any]]] = {}
        for vr in result.get("valueRanges", []) or []:
            rng = vr.get("range") or ""
            if "!" in rng:
                tab = rng.split("!", 1)[0].strip().strip("'\"")
            else:
                tab = rng.strip().strip("'\"")
            if tab:
                out[tab] = vr.get("values", []) or []
        return out

    def sync_offers_and_signals(
        self,
        row: List[Any],
        cian_id: str,
        id_column_index: int,
        offers_bg_color: Optional[dict],
        signals_match: bool,
        signals_bg_color: dict,
    ) -> bool:
        """Один batchGet для Offers_Parser + Signals_Parser, затем обновления (меньше read quota)."""
        by_tab = self.batch_get_value_ranges(
            ["Offers_Parser!A:Z", "Signals_Parser!A:Z"]
        )
        offers_vals = by_tab.get("Offers_Parser") or []
        signals_vals = by_tab.get("Signals_Parser") or []
        ok_offers = self.find_and_update_row(
            "Offers_Parser",
            row,
            id_value=cian_id,
            id_column_index=id_column_index,
            bg_color=offers_bg_color,
            existing_values=offers_vals,
        )
        if signals_match:
            self.find_and_update_row(
                "Signals_Parser",
                row,
                id_value=cian_id,
                id_column_index=id_column_index,
                bg_color=signals_bg_color,
                existing_values=signals_vals,
            )
        else:
            self.delete_row_by_id(
                "Signals_Parser",
                id_value=cian_id,
                id_column_index=id_column_index,
                existing_values=signals_vals,
            )
        return ok_offers

    def get_urls(self, tab_name: str = "FILTERS", column: str = "A") -> List[str]:
        """
        Читает URLs из указанной табы и колонки.

        Важно: values().get по умолчанию отдаёт *отображаемый* текст. Для =HYPERLINK(...,"короткая подпись")
        или вставленной в текст ссылки это не полный URL. Поэтому используем spreadsheets.get +
        includeGridData: поле hyperlink, textFormatRuns, formulaValue.

        Args:
            tab_name: Название табы (по умолчанию "FILTERS")
            column: Колонка с URLs (по умолчанию "A")

        Returns:
            Список валидных URL в порядке строк листа
        """
        try:
            self._throttle_read()
            range_a1 = f"{tab_name}!{column}1:{column}{_SHEETS_URL_COLUMN_MAX_ROWS}"

            def _get_grid():
                # Вся CellData по колонке: hyperlink, formula, rich-text links (textFormatRuns)
                return (
                    self.sheet.get(
                        spreadsheetId=self.spreadsheet_id,
                        ranges=[range_a1],
                        includeGridData=True,
                        fields="sheets(data(rowData(values)))",
                    ).execute()
                )

            grid = self._execute_with_retry(_get_grid)
            urls: List[str] = []
            for sheet in grid.get("sheets") or []:
                for data in sheet.get("data") or []:
                    for row in data.get("rowData") or []:
                        if not row:
                            continue
                        cells = row.get("values") or []
                        if not cells:
                            continue
                        url = _url_from_grid_cell(cells[0])
                        if not url:
                            continue
                        low = url.lower()
                        if low in ("url", "urls"):
                            continue
                        urls.append(url)

            logger.info(f"Read {len(urls)} URLs from {tab_name}!{column} (gridData/hyperlink)")
            return urls

        except Exception as e:
            logger.error(f"Failed to read URLs from Google Sheets: {e}")
            raise
    def write_row(
        self,
        tab_name: str,
        row: List[Any],
        clear_format: bool = False,
        insert_at_top: bool = True,
        bg_color: dict = None,
    ) -> bool:
        """Добавляет одну строку в таблицу.

        Важно: вставка "наверх" через insertDimension + update в A2.
        Это надежнее, чем values.append, который всегда добавляет вниз.

        Args:
            tab_name: Название табы (например "RESULTS", "PARSED")
            row: Список значений для строки
            clear_format: Если True, игнорирует форматирование ячеек (пока не используется)
            insert_at_top: Если True, вставляет строку после заголовка (строка 2)
            bg_color: Dict с цветом заливки (rgb), например {"red": 1.0, "green": 0.0, "blue": 0.0}

        Returns:
            True если успешно, False если ошибка
        """
        try:
            if insert_at_top:
                sheet_id = self._get_sheet_id(tab_name)
                logger.info(f"Inserting row at top of {tab_name} (sheet_id={sheet_id})")

                requests = [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": 1,
                                "endIndex": 2,
                            },
                            "inheritFromBefore": False,
                        }
                    }
                ]

                # Всегда обновляем цвет, чтобы сбросить унаследованный цвет от строки ниже
                bg_color_dict = bg_color if bg_color else {"red": 1.0, "green": 1.0, "blue": 1.0}
                
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": bg_color_dict.get("red", 1.0),
                                    "green": bg_color_dict.get("green", 1.0),
                                    "blue": bg_color_dict.get("blue", 1.0)
                                }
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

                max_retries = 3
                for attempt in range(max_retries):
                    try:

                        def _ins():
                            return (
                                self.service.spreadsheets()
                                .batchUpdate(
                                    spreadsheetId=self.spreadsheet_id,
                                    body={"requests": requests},
                                )
                                .execute()
                            )

                        self._execute_with_retry(_ins)
                        logger.info("✓ Inserted empty row at position 2")

                        body = {"values": [row]}

                        def _upd():
                            return (
                                self.sheet.values()
                                .update(
                                    spreadsheetId=self.spreadsheet_id,
                                    range=f"{tab_name}!A2",
                                    valueInputOption="USER_ENTERED",
                                    body=body,
                                )
                                .execute()
                            )

                        result = self._execute_with_retry(_upd)

                        updated = result.get("updatedRows", 0)
                        if updated > 0:
                            logger.info(f"✓ Wrote data to A2, {updated} rows updated")
                        return updated > 0
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(
                                f"Failed to write row (attempt {attempt+1}/{max_retries}): {e}. Retrying..."
                            )
                            time.sleep(1)
                        else:
                            raise e

            # insert_at_top=False -> обычный append вниз
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    body = {"values": [row]}

                    def _app():
                        return (
                            self.sheet.values()
                            .append(
                                spreadsheetId=self.spreadsheet_id,
                                range=f"{tab_name}!A:Z",
                                valueInputOption="USER_ENTERED",
                                insertDataOption="INSERT_ROWS",
                                body=body,
                            )
                            .execute()
                        )

                    result = self._execute_with_retry(_app)
                    updates = result.get("updates", {}) or {}
                    return (updates.get("updatedRows", 0) or 0) > 0
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Failed to append row (attempt {attempt+1}/{max_retries}): {e}. Retrying..."
                        )
                        time.sleep(1)
                    else:
                        raise e

        except Exception as e:
            logger.error(f"Failed to write row to Google Sheets: {e}", exc_info=True)
            return False

    def find_and_update_row(
        self,
        tab_name: str,
        row: List[Any],
        id_value: str,
        id_column_index: int = 0,
        bg_color: dict = None,
        existing_values: Optional[List[List[Any]]] = None,
    ) -> bool:
        """Ищет строку по ID в указанной колонке и обновляет её.
        Если не находит - вставляет новую наверх.

        Args:
            tab_name: Название табы
            row: Данные строки
            id_value: Значение ID для поиска (например cian_id)
            id_column_index: Индекс колонки с ID (0 = A)
            bg_color: Цвет фона для обновления
            existing_values: Уже загруженные строки листа (из batchGet) — без лишнего read

        Returns:
            True если успешно
        """
        try:
            if existing_values is not None:
                values = existing_values
            else:
                self._throttle_read()

                def _get_vals():
                    return (
                        self.sheet.values()
                        .get(
                            spreadsheetId=self.spreadsheet_id,
                            range=f"{tab_name}!A:Z",
                        )
                        .execute()
                    )

                result = self._execute_with_retry(_get_vals)
                values = result.get("values", [])
            row_index = -1
            
            # Пропускаем первую строку (заголовок)
            for i, existing_row in enumerate(values):
                if i == 0:
                    continue
                if len(existing_row) > id_column_index and str(existing_row[id_column_index]) == str(id_value):
                    row_index = i + 1 # 1-based index
                    break
            
            if row_index != -1:
                # 2. Нашли! Обновляем существующую
                logger.info(f"Found existing row for {id_value} at index {row_index} in {tab_name}. Updating...")
                
                # Обновляем значения
                body = {"values": [row]}

                def _put_vals():
                    return (
                        self.sheet.values()
                        .update(
                            spreadsheetId=self.spreadsheet_id,
                            range=f"{tab_name}!A{row_index}",
                            valueInputOption="USER_ENTERED",
                            body=body,
                        )
                        .execute()
                    )

                self._execute_with_retry(_put_vals)

                # Обновляем цвет: если bg_color не задан, сбрасываем в белый
                bg_color_dict = bg_color if bg_color else {"red": 1.0, "green": 1.0, "blue": 1.0}
                sheet_id = self._get_sheet_id(tab_name)
                requests = [{
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index - 1,
                            "endRowIndex": row_index,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": bg_color_dict.get("red", 1.0),
                                    "green": bg_color_dict.get("green", 1.0),
                                    "blue": bg_color_dict.get("blue", 1.0)
                                }
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                }]

                def _fmt():
                    return (
                        self.service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body={"requests": requests},
                        )
                        .execute()
                    )

                self._execute_with_retry(_fmt)
                
                return True
            else:
                # 3. Не нашли - вставляем новую наверх
                return self.write_row(tab_name, row, insert_at_top=True, bg_color=bg_color)
                
        except Exception as e:
            logger.error(f"Failed to find_and_update_row for {id_value} in {tab_name}: {e}")
            return False

    def delete_row_by_id(
        self,
        tab_name: str,
        id_value: str,
        id_column_index: int = 0,
        existing_values: Optional[List[List[Any]]] = None,
    ) -> bool:
        """Удаляет строку с заданным ID в колонке. Если строки нет — True (идемпотентно)."""
        try:
            if existing_values is not None:
                values = existing_values
            else:
                self._throttle_read()

                def _get_vals():
                    return (
                        self.sheet.values()
                        .get(
                            spreadsheetId=self.spreadsheet_id,
                            range=f"{tab_name}!A:Z",
                        )
                        .execute()
                    )

                result = self._execute_with_retry(_get_vals)
                values = result.get("values", [])
            row_index = -1
            for i, existing_row in enumerate(values):
                if i == 0:
                    continue
                if (
                    len(existing_row) > id_column_index
                    and str(existing_row[id_column_index]) == str(id_value)
                ):
                    row_index = i + 1
                    break
            if row_index == -1:
                return True
            sheet_id = self._get_sheet_id(tab_name)

            def _del():
                return (
                    self.service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={
                            "requests": [
                                {
                                    "deleteDimension": {
                                        "range": {
                                            "sheetId": sheet_id,
                                            "dimension": "ROWS",
                                            "startIndex": row_index - 1,
                                            "endIndex": row_index,
                                        }
                                    }
                                }
                            ]
                        },
                    )
                    .execute()
                )

            self._execute_with_retry(_del)
            logger.info(f"Deleted row for id={id_value} from {tab_name} (sheet row {row_index})")
            return True
        except Exception as e:
            logger.error(f"Failed to delete_row_by_id for {id_value} in {tab_name}: {e}")
            return False

    def _get_sheet_id(self, tab_name: str) -> int:
        """ID листа по имени (кэш + один metadata read на всю книгу)."""
        if tab_name in self._sheet_id_cache:
            return self._sheet_id_cache[tab_name]
        self._throttle_read()

        def _meta():
            return (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )

        sheet_metadata = self._execute_with_retry(_meta)
        for sheet in sheet_metadata.get("sheets", []):
            t = sheet["properties"]["title"]
            self._sheet_id_cache[t] = sheet["properties"]["sheetId"]
        if tab_name not in self._sheet_id_cache:
            raise ValueError(f"Sheet '{tab_name}' not found")
        return self._sheet_id_cache[tab_name]

    def write_rows(
        self, 
        tab_name: str, 
        rows: List[List[Any]],
        clear_format: bool = False,
        insert_at_top: bool = True
    ) -> bool:
        """
        Добавляет несколько строк в таблицу (batch операция).
        
        Args:
            tab_name: Название табы
            rows: Список списков значений
            clear_format: Если True, игнорирует форматирование
            insert_at_top: Если True, вставляет строки после заголовка
            
        Returns:
            True если успешно, False если ошибка
        """
        if not rows:
            logger.debug(f"No rows to write to {tab_name}")
            return True

        try:
            if insert_at_top:
                # Вставляем строки после заголовка
                for row in reversed(rows):
                    self.write_row(tab_name, row, clear_format, insert_at_top=True)
                return True
            else:
                body = {"values": rows}
                result = (
                    self.sheet.values()
                    .append(
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{tab_name}!A:Z",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body=body,
                    )
                    .execute()
                )
                success = "updates" in result and result["updates"]["updatedRows"] > 0
                if success:
                    logger.info(f"Wrote {len(rows)} rows to {tab_name}")
                return success
            
        except Exception as e:
            logger.error(f"Failed to write {len(rows)} rows to Google Sheets: {e}")
            return False

    def read_range(self, range_str: str) -> List[List[Any]]:
        """
        Читает произвольный диапазон из таблицы.
        
        Args:
            range_str: Диапазон в формате "TabName!A1:B10"
            
        Returns:
            Двумерный список значений
        """
        try:
            self._throttle_read()

            def _rg():
                return (
                    self.sheet.values()
                    .get(spreadsheetId=self.spreadsheet_id, range=range_str)
                    .execute()
                )

            result = self._execute_with_retry(_rg)
            values = result.get("values", [])
            logger.debug(f"Read {len(values)} rows from {range_str}")
            return values

        except Exception as e:
            logger.error(f"Failed to read range {range_str}: {e}")
            return []

    def update_range(self, range_str: str, values: List[List[Any]]) -> bool:
        """
        Обновляет значения в указанном диапазоне.
        
        Args:
            range_str: Диапазон в формате "TabName!A1:B10"
            values: Двумерный список новых значений
            
        Returns:
            True если успешно, False если ошибка
        """
        try:
            body = {"values": values}
            result = (
                self.sheet.values()
                .update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_str,
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            success = "updatedRows" in result and result["updatedRows"] > 0
            if success:
                logger.info(f"Updated range {range_str}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to update range {range_str}: {e}")
            return False

    def clear_range(self, range_str: str) -> bool:
        """
        Очищает значения в указанном диапазоне.
        
        Args:
            range_str: Диапазон в формате "TabName!A1:B10"
            
        Returns:
            True если успешно, False если ошибка
        """
        try:
            result = (
                self.sheet.values()
                .clear(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_str,
                )
                .execute()
            )
            success = "clearedRows" in result
            if success:
                logger.info(f"Cleared range {range_str}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to clear range {range_str}: {e}")
            return False

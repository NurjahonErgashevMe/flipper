"""
SheetsManager - Generic Google Sheets API wrapper

Данный модуль абстрагирован от любых специфических моделей данных.
Используется любым парсером для чтения URLs и записи результатов.
"""

import os
import logging
from typing import List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


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
        
        logger.info(f"SheetsManager initialized for spreadsheet {spreadsheet_id}")

    def get_urls(self, tab_name: str = "FILTERS", column: str = "A") -> List[str]:
        """
        Читает URLs из указанной табы и колонки.
        Автоматически фильтрует заголовки и невалидные URL.
        
        Args:
            tab_name: Название табы (по умолчанию "FILTERS")
            column: Колонка с URLs (по умолчанию "A")
            
        Returns:
            Список валидных URL
        """
        try:
            result = (
                self.sheet.values()
                .get(spreadsheetId=self.spreadsheet_id, range=f"{tab_name}!{column}:{column}")
                .execute()
            )
            values = result.get("values", [])
            
            urls = []
            for row in values:
                if row and row[0]:
                    url = row[0].strip()
                    # Пропускаем заголовки и невалидные URLs
                    if (
                        url.lower() not in ("url", "urls", "")
                        and url.startswith("http")
                    ):
                        urls.append(url)
            
            logger.info(f"Read {len(urls)} URLs from {tab_name}!{column}")
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

                import time
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # 1) Вставляем новую строку на позицию 2 (index=1) и красим если нужно
                        self.service.spreadsheets().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body={"requests": requests},
                        ).execute()
                        logger.info(f"✓ Inserted empty row at position 2")
        
                        # 2) Пишем значения в A2
                        body = {"values": [row]}
                        result = (
                            self.sheet.values()
                            .update(
                                spreadsheetId=self.spreadsheet_id,
                                range=f"{tab_name}!A2",
                                valueInputOption="USER_ENTERED",
                                body=body,
                            )
                            .execute()
                        )
        
                        updated = result.get("updatedRows", 0)
                        if updated > 0:
                            logger.info(f"✓ Wrote data to A2, {updated} rows updated")
                        return updated > 0
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Failed to write row (attempt {attempt+1}/{max_retries}): {e}. Retrying...")
                            time.sleep(1)
                        else:
                            raise e

            # insert_at_top=False -> обычный append вниз
            import time
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    body = {"values": [row]}
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
                    updates = result.get("updates", {}) or {}
                    return (updates.get("updatedRows", 0) or 0) > 0
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Failed to append row (attempt {attempt+1}/{max_retries}): {e}. Retrying...")
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
        bg_color: dict = None
    ) -> bool:
        """Ищет строку по ID в указанной колонке и обновляет её.
        Если не находит - вставляет новую наверх.

        Args:
            tab_name: Название табы
            row: Данные строки
            id_value: Значение ID для поиска (например cian_id)
            id_column_index: Индекс колонки с ID (0 = A)
            bg_color: Цвет фона для обновления

        Returns:
            True если успешно
        """
        try:
            # 1. Сначала пытаемся найти строку
            result = self.sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab_name}!A:Z"
            ).execute()
            
            values = result.get("values", [])
            row_index = -1
            
            # Пропускаем первую строку (заголовок)
            for i, existing_row in enumerate(values):
                if i == 0: continue
                if len(existing_row) > id_column_index and str(existing_row[id_column_index]) == str(id_value):
                    row_index = i + 1 # 1-based index
                    break
            
            if row_index != -1:
                # 2. Нашли! Обновляем существующую
                logger.info(f"Found existing row for {id_value} at index {row_index} in {tab_name}. Updating...")
                
                # Обновляем значения
                body = {"values": [row]}
                self.sheet.values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{tab_name}!A{row_index}",
                    valueInputOption="USER_ENTERED",
                    body=body
                ).execute()
                
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
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": requests}
                ).execute()
                
                return True
            else:
                # 3. Не нашли - вставляем новую наверх
                return self.write_row(tab_name, row, insert_at_top=True, bg_color=bg_color)
                
        except Exception as e:
            logger.error(f"Failed to find_and_update_row for {id_value} in {tab_name}: {e}")
            return False

    def _get_sheet_id(self, tab_name: str) -> int:
        """Получает ID листа по имени"""
        import time
        max_retries = 3
        for attempt in range(max_retries):
            try:
                sheet_metadata = self.service.spreadsheets().get(
                    spreadsheetId=self.spreadsheet_id
                ).execute()
                for sheet in sheet_metadata.get("sheets", []):
                    if sheet["properties"]["title"] == tab_name:
                        return sheet["properties"]["sheetId"]
                raise ValueError(f"Sheet '{tab_name}' not found")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Failed to get sheet ID (attempt {attempt+1}/{max_retries}): {e}. Retrying...")
                    time.sleep(1)
                else:
                    logger.error(f"Failed to get sheet ID: {e}")
                    raise

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
            result = (
                self.sheet.values()
                .get(spreadsheetId=self.spreadsheet_id, range=range_str)
                .execute()
            )
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

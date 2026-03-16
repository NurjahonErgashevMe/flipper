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

    def __init__(self, spreadsheet_id: str, credentials_path: str):
        """
        Args:
            spreadsheet_id: ID документа Google Sheets
            credentials_path: Путь к JSON ключу Service Account
        """
        self.spreadsheet_id = spreadsheet_id
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Credentials file not found at {credentials_path}. "
                f"Please ensure you have Google Service Account JSON key."
            )

        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        self.service = build("sheets", "v4", credentials=self.credentials)
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
        clear_format: bool = False
    ) -> bool:
        """
        Добавляет одну строку в конец таблицы.
        
        Args:
            tab_name: Название табы (например "RESULTS", "PARSED")
            row: Список значений для строки
            clear_format: Если True, игнорирует форматирование ячеек
            
        Returns:
            True если успешно, False если ошибка
        """
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
            success = "updates" in result and result["updates"]["updatedRows"] > 0
            if success:
                logger.debug(f"Wrote 1 row to {tab_name}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to write row to Google Sheets: {e}")
            return False

    def write_rows(
        self, 
        tab_name: str, 
        rows: List[List[Any]],
        clear_format: bool = False
    ) -> bool:
        """
        Добавляет несколько строк в конец таблицы (batch операция).
        
        Args:
            tab_name: Название табы
            rows: Список списков значений
            clear_format: Если True, игнорирует форматирование
            
        Returns:
            True если успешно, False если ошибка
        """
        if not rows:
            logger.debug(f"No rows to write to {tab_name}")
            return True

        try:
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

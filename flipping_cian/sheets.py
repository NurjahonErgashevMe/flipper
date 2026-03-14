import os
from typing import List
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsManager:
    def __init__(self, spreadsheet_id: str, credentials_path: str):
        self.spreadsheet_id = spreadsheet_id
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials file not found at {credentials_path}")

        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        self.service = build("sheets", "v4", credentials=self.credentials)
        self.sheet = self.service.spreadsheets()

    def get_urls(self) -> List[str]:
        """Reads URLs from the FILTERS tab, column A"""
        try:
            result = (
                self.sheet.values()
                .get(spreadsheetId=self.spreadsheet_id, range="FILTERS!A:A")
                .execute()
            )
            values = result.get("values", [])

            urls = []
            for row in values:
                if row and row[0]:
                    url = row[0].strip()
                    # Skip header if it exists
                    if (
                        url.lower() != "url"
                        and url.lower() != "urls"
                        and url.startswith("http")
                    ):
                        urls.append(url)
            return urls
        except Exception as e:
            logger.error(f"Failed to read URLs from Google Sheets: {e}")
            raise

    def write_parsed_row(self, row: List[str]) -> bool:
        """Appends a single parsed row to the PARSED tab"""
        try:
            body = {"values": [row]}
            result = (
                self.sheet.values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range="PARSED!A:V",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                )
                .execute()
            )
            return "updates" in result
        except Exception as e:
            logger.error(f"Failed to append row to Google Sheets: {e}")
            return False

    def write_parsed_rows(self, rows: List[List[str]]) -> bool:
        """Appends multiple parsed rows to the PARSED tab"""
        if not rows:
            return True

        try:
            body = {"values": rows}
            result = (
                self.sheet.values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range="PARSED!A:V",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                )
                .execute()
            )
            return "updates" in result
        except Exception as e:
            logger.error(f"Failed to append rows to Google Sheets: {e}")
            return False

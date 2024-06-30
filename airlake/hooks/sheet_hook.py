import logging
from string import ascii_uppercase
from functools import cached_property

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook as GoogleCloudBaseHook
import gspread
from google.auth.transport.requests import AuthorizedSession
from gspread_dataframe import set_with_dataframe
import pandas


class SheetWriteDisposition:
    TRUNCATE = "TRUNCATE"
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    COLUMN_TRUNCATE = "COLUMN_TRUNCATE"

    @classmethod
    def from_string(cls, s):
        if s == "TRUNCATE":
            return cls.TRUNCATE
        if s == "OVERWRITE":
            return cls.OVERWRITE
        if s == "APPEND":
            return cls.APPEND
        if s == "COLUMN_TRUNCATE":
            return cls.COLUMN_TRUNCATE


class SheetBase:
    client: gspread.Client

    def upload_df_to_sheet(
        self,
        dataframe,
        destination_sheet_id=None,
        destination_sheet_uri=None,
        destination_worksheet_name="Sheet1",
        row=1,
        col=1,
        include_index=False,
        include_column_header=True,
        allow_formulas=True,
        write_disposition=SheetWriteDisposition.OVERWRITE,
    ):
        worksheet = self.get_worksheet(
            destination_sheet_id or destination_sheet_uri, destination_worksheet_name
        )
        if write_disposition == SheetWriteDisposition.TRUNCATE:
            worksheet.clear()
            logging.info("Successfully clear sheet {}".format(worksheet))
        if write_disposition == SheetWriteDisposition.APPEND:
            col_values = worksheet.col_values(col)
            last_row_with_value = len(col_values)
            row = last_row_with_value + 1
        if write_disposition == SheetWriteDisposition.COLUMN_TRUNCATE:
            col_values = worksheet.col_values(col)
            last_row_with_value = len(col_values)
            range_notation = range_indices_to_notation(
                row,
                col,
                last_row_with_value,
                len(dataframe.columns),
            )
            worksheet.spreadsheet.values_clear(range_notation)
            logging.info("Successfully clear range %s", range_notation)

        set_with_dataframe(
            worksheet,
            dataframe,
            row=row,
            col=col,
            include_index=include_index,
            include_column_header=include_column_header,
            allow_formulas=allow_formulas,
        )

        logging.info("Success uploading data to worksheet")

    def get_df(
        self,
        sheet_id_or_url: str,
        worksheet_name: str,
    ) -> pandas.DataFrame:
        worksheet = self.get_worksheet(sheet_id_or_url, worksheet_name)
        records = worksheet.get_all_records()
        return pandas.DataFrame.from_dict(records)

    def get_worksheet(
        self, sheet_id_or_url: str, worksheet_name: str
    ) -> gspread.Worksheet:
        if not (sheet_id_or_url):
            raise Exception("Missing sheet_id_or_url")
        if sheet_id_or_url.startswith("https://"):
            sheet = self.client.open(sheet_id_or_url)
        else:
            sheet = self.client.open_by_key(sheet_id_or_url)
        return sheet.worksheet(worksheet_name)


class SheetHook(GoogleCloudBaseHook, SheetBase):
    def __init__(self, sheet_conn_id="sheet_default", delegate_to=None):
        super(SheetHook, self).__init__(
            gcp_conn_id=sheet_conn_id, delegate_to=delegate_to
        )

    @cached_property
    def client(self):
        credentials = self._get_credentials()
        client = gspread.Client(
            auth=credentials,
            session=AuthorizedSession(credentials=credentials),
        )
        client.set_timeout(7200)
        return client


def range_indices_to_notation(
    row_start: int, col_start: int, row_end: int, col_end: int
) -> str:
    return "{}{}:{}{}".format(
        _number_to_char(col_start - 1), row_start, _number_to_char(col_end - 1), row_end
    )


def _number_to_char(num: int):
    chars = {}
    for i, c in enumerate(ascii_uppercase):
        chars.update({i: c})
    sb = []
    if num == 0:
        return "A"
    while num != 0:
        idx = num % 26
        if int(num / 26) == 0 and len(sb) > 0:
            idx -= 1
        letter = chars.get(idx)
        sb.insert(0, letter)
        num = int(num / 26)
    res = "".join(sb)
    return res


if __name__ == "__main__":
    import sys

    # _number_to_char(int(sys.argv[1]))

    res = range_indices_to_notation(
        int(sys.argv[1]),
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
    )
    print(res)

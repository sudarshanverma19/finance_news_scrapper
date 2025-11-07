import os

import pandas as pd
from fake_useragent import UserAgent

ua = UserAgent()


def get_spider_output(output_file: str) -> pd.DataFrame:
    """Returns the saved spider output as a pandas DataFrame"""

    if os.path.isfile(output_file):
        df = pd.read_csv(output_file)

        # parse some required date columns
        for c in ["date_published", "date_modified"]:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
            df[c] = df[c].dt.tz_localize(None)

    else:
        df = pd.DataFrame()

    return df

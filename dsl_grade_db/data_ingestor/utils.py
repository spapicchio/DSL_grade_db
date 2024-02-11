import pandas as pd


def read_file(path):
    # based on path, read csv or excel
    if path.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)
    return df

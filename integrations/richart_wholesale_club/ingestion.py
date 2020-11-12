import os
import pathlib
import pandas as pd
import re
from typing import List, Dict
import sys

SCRIPT_PATH = pathlib.Path(__file__).parent.absolute()
BRANCH_PATH = SCRIPT_PATH / '../../assets/PRICES-STOCK.csv'
PRODUCT_PATH = SCRIPT_PATH / '../../assets/PRODUCTS.csv'
DB_PATH = SCRIPT_PATH / '../../db.sqlite'

sys.path.append(str(SCRIPT_PATH / '../..'))
from database_setup import engine as ENGINE

STORE = "Richart's"
PRODUCT_COLS_READ = ['SKU', 'BUY_UNIT', 'BARCODES', 'NAME', 'DESCRIPTION',
                     'IMAGE_URL', 'CATEGORY', 'BRAND', 'SUB_CATEGORY', 'SUB_SUB_CATEGORY']
USE_BRANCHES = ['mm', 'rhsm']

SKU_COL = 'product_id'
BRANCH_COL = 'branch'
PRICE_COL = 'price'
CATEGORY_COL = 'category'
SUB_CATEGORY_COL = 'sub_category'
SUB_SUB_CATEGORY_COL = 'sub_sub_category'
STOCK_COL = 'stock'
DESCRIPTION_COL = 'description'
STORE_COL = 'store'
NAME_COL = 'name'
PACKAGE_COL = 'package'

PRODUCT_RENAME_COLS = {'buy_unit': 'package'}
BRANCH_RENAME_COL = {'sku': 'product_id'}


class DataProcessing:
    def __init__(self, path: str, table: str = None, rename: Dict[str, str] = None, db_engine=None, **kwargs) -> None:
        self.data = pd.read_csv(path, **kwargs)
        self.table = table
        self.rename = rename
        self.engine = db_engine

    def pre_processing(self):
        """Pre processes converint cols and content to lowercase, renaming cols and processing object columns"""
        # Column names to lower case
        self._cols_name_lowercase()
        # Rename columns
        if self.rename:
            self.data.rename(self.rename, axis=1, inplace=True)
        self._process_text_cols()

    def _cols_name_lowercase(self):
        self.data.columns = [col.lower() for col in self.data.columns]

    def _process_text_cols(self):
        cols = self.data.select_dtypes('object').columns
        for col in cols:
            # Remove spaces in the beginning or end of string
            self.data[col] = self.data[col].str.strip()
            # Remove multiple spaces
            self.data[col] = self.data[col].str.replace('\s+', ' ')
            # Remove the comma character
            self.data[col] = self.data[col].str.replace(',', '')
            # Remove the dot character
            self.data[col] = self.data[col].str.replace('.', '', regex=False)
            # Transform to lower case
            self.data[col] = self.data[col].str.lower()
            # Remove HTML tags
            self.data[col] = self.data[col].str.replace('<\w+>', '')
            self.data[col] = self.data[col].str.replace('</\w+>', '')

    def concat_cols(self, new_col: str, cols_join: List[str], sep: str = '|'):
        if new_col not in self.data.columns:
            self.data[new_col] = ''
        for col in cols_join:
            self.data[new_col] = self.data[new_col] + sep + self.data[col]

    def save_to_db(self):
        if not self.table:
            raise Exception('A table name must be provided')
        if not self.engine:
            raise Exception('A database engine must be provided')
        sqlite_connection = self.engine.connect()
        self.data.to_sql(self.table, sqlite_connection,
                         index=False, if_exists='append')
        sqlite_connection.close()


class ProductProcessing(DataProcessing):
    def _get_package(self):
        """Extract buy unit from description"""
        def extract_buy_unit(val: str):
            if val == 'un':
                return 'un'
            elif re.search(r'\bpza\b', val):
                return 'un'
            elif re.search(r'\bgranel\b', val):
                return 'kg'
            elif re.search(r'\b100\s?gr?s?\b', val):
                return 'kg'
            elif re.search(r'\b1\s?kg\b', val):
                return 'kg'
            else:
                return 'un'
        # Fill package
        self.data[PACKAGE_COL] = self.data[DESCRIPTION_COL].map(
            extract_buy_unit)

    def process(self):
        self.pre_processing()
        # Set Store column
        self.data[STORE_COL] = STORE
        # Fill NAME_COL
        filter = self.data[NAME_COL].isnull()
        self.data.loc[filter, NAME_COL] = self.data[filter][DESCRIPTION_COL]
        # Join Categories
        self.concat_cols(new_col=CATEGORY_COL, cols_join=[
            CATEGORY_COL, SUB_CATEGORY_COL, SUB_SUB_CATEGORY_COL])
        # Remove columns that won't be stored in database
        self.data.drop([SUB_CATEGORY_COL, SUB_SUB_CATEGORY_COL],
                       axis=1, inplace=True)
        self._get_package()


class BranchProcessing(DataProcessing):
    def process(self):
        self.pre_processing()
        # Filter branches
        self.data = self.data[self.data[BRANCH_COL].isin(USE_BRANCHES)]
        # Filter out negative stock
        self.data = self.data[self.data[STOCK_COL] > 0]
        # Aggregate rows with the same SKU and BRANCH
        self.data = self.data.groupby([SKU_COL, BRANCH_COL], as_index=False).agg({
            PRICE_COL: 'min', STOCK_COL: 'sum'})


if __name__ == "__main__":
    product = ProductProcessing(path=PRODUCT_PATH, table='products',
                                rename=PRODUCT_RENAME_COLS, db_engine=ENGINE, usecols=PRODUCT_COLS_READ, sep='|')
    product.process()
    product.save_to_db()
    branch = BranchProcessing(
        path=BRANCH_PATH, table='branchproducts', rename=BRANCH_RENAME_COL, db_engine=ENGINE, sep='|')
    branch.process()
    branch.save_to_db()

import pandas as pd
import numpy as np
import argparse

class SalesAnalyzer:
    def __init__(self, brand_file, product_file, store_file, sales_file):
        self.brands = pd.read_csv(brand_file)
        self.products = pd.read_csv(product_file)
        self.stores = pd.read_csv(store_file)
        self.sales = pd.read_csv(sales_file)

    # def preprocess_data(self, min_date, max_date):
    def preprocess_data(self):
        self.sales['date'] = pd.to_datetime(self.sales['date'])
        # self.sales = self.sales[(self.sales['date'] >= min_date) & (self.sales['date'] <= max_date)]

    def calculate_features(self):
        # Merge 'sales' DataFrame with 'product' and 'brand' DataFrames to get the 'name' column from 'brand.csv'

        self.products = pd.merge(self.products, self.brands[['name', 'id']], left_on='brand', right_on='name', how='left')
        self.products.columns = ['product_name', 'product_brand_name', 'product_id', 'brand_name', 'brand_id']
        # self.products.drop(columns=['product_brand_name'])

        self.sales = pd.merge(self.sales, self.products[['product_name', 'product_id',
                                    'brand_name', 'brand_id']], left_on='product', right_on='product_id', how='left')

        self.stores.columns = ['store_name', 'store_city', 'store_id']
        self.sales = pd.merge(self.sales, self.stores[['store_name', 'store_city',
                                    'store_id']], left_on='store', right_on='store_id', how='left')

        # Calculate sales_product
        self.sales['sales_product'] = self.sales['quantity']

        # Calculate MA7_P
        self.sales['MA7_P'] = self.sales['sales_product'].rolling(window=7).mean().reset_index(
            level=0, drop=True).shift(1).astype(float)
        # Calculate LAG7_P
        self.sales['LAG7_P'] = self.sales['sales_product'].shift(7).astype(float)
        # Calculate slase_brand
        self.sales['sales_brand'] = self.sales.groupby(['brand_name', 'store_name', 'date'])['quantity'].transform('sum')
        # Calculate MA7_B
        self.sales['MA7_B'] = self.sales['sales_brand'].rolling(
            window=7).mean().reset_index(level=0, drop=True).shift(1).astype(float)
        # Calculate LAG7_B
        self.sales['LAG7_B'] = self.sales.groupby(['brand_name', 'store_name'])['sales_brand'].shift(7).astype(float)
        # Calculate sales_store
        self.sales['sales_store'] = self.sales.groupby(['store_name', 'date'])['quantity'].transform('sum')
        # Calculate MA7_S
        self.sales['MA7_S'] = self.sales.groupby('store_name')['sales_store'].rolling(
            window=7).mean().reset_index(level=0, drop=True).shift(1).astype(float)

        self.sales['LAG7_S'] = self.sales.groupby('store_name')['sales_store'].shift(7).astype(float)

        # print("here")

    def calculate_wmape(self, top_n, min_date, max_date):
        self.sales = self.sales[(self.sales['date'] >= min_date) & (self.sales['date'] <= max_date)]

        self.sales['abs_error'] = np.abs(self.sales['sales_product'] - self.sales['MA7_P'])
        self.sales['wmape'] = self.sales['abs_error'] / np.abs(self.sales['sales_product'])

        self.wmape_results = self.sales.groupby(['product_id', 'store_id', 'brand_id'])['wmape'].mean().reset_index()
        self.wmape_results = self.sales[np.isfinite(self.sales['wmape'])]
        self.wmape_results = self.wmape_results.sort_values(by='wmape', ascending=False).head(top_n)
        # print("here")

    def save_features_csv(self, output_filename, min_date, max_date):
        sales_filtered = self.sales[(self.sales['date'] >= min_date) & (self.sales['date'] <= max_date)]
        sales_filtered.to_csv(output_filename, columns=[
            'product_id', 'store_id', 'brand_id', 'date', 'sales_product', 'MA7_P',
            'LAG7_P', 'sales_brand', 'MA7_B', 'LAG7_B', 'sales_store', 'MA7_S', 'LAG7_S'
        ], index=False)

    def save_wmape_csv(self, output_filename):
        self.wmape_results.to_csv(output_filename, columns=[
            'product_id', 'store_id', 'brand_id', 'wmape'
        ], index=False)

    def print_features(self, top_n):
        # Read the data from "features.csv"
        features_df = pd.read_csv('features.csv')
        # print(features_df.head(top_n))

        print("\n --Output1 to be written to: features.csv--")
        features_df_column_names = list(features_df.columns)
        print(features_df_column_names)

        # Select and print the top N rows
        top_n_rows = features_df.head(top_n)
        for _, row in top_n_rows.iterrows():
            print(
                f"{int(row['product_id'])},{int(row['store_id'])},{int(row['brand_id'])},{row['date']},"
                f"{int(row['sales_product'])},{row['MA7_P']},{int(row['LAG7_P'])},{int(row['sales_brand'])},"
                f"{row['MA7_B']},{int(row['LAG7_B'])},{int(row['sales_store'])},{row['MA7_S']},{int(row['LAG7_S'])}")

        print("\n --Output2 to be written to: mapes.csv--")
        wmape_df = pd.read_csv('mapes.csv')
        wmape_df_column_names = list(wmape_df.columns)
        print(wmape_df_column_names)

        # Select and print the top N rows
        top_n_rows_wmape = wmape_df.head(top_n)
        for w_, w_row in top_n_rows_wmape.iterrows():
            print(f"{int(w_row['product_id'])},{int(w_row['store_id'])},{int(w_row['brand_id'])},{w_row['wmape']}")

def main():
    parser = argparse.ArgumentParser(description="Sales Analyzer")
    parser.add_argument("--min-date", type=str, default="2021-01-08", help="Start date of the date range (YYYY-MM-DD)")
    parser.add_argument("--max-date", type=str, default="2021-05-30", help="End date of the date range (YYYY-MM-DD)")
    parser.add_argument("--top", type=int, default=5, help="Number of rows in the WMAPE output")
    args = parser.parse_args()

    """min_date = '2021-01-08'
    max_date = '2021-05-30'
    top_n = 5"""

    brand_file = './input_data/data/brand.csv'
    product_file = './input_data/data/product.csv'
    store_file = './input_data/data/store.csv'
    sales_file = './input_data/data/sales.csv'

    analyzer = SalesAnalyzer(brand_file, product_file, store_file, sales_file)
    # analyzer.preprocess_data(args.min_date, args.max_date)
    analyzer.preprocess_data()
    analyzer.calculate_features()
    analyzer.save_features_csv('features.csv',args.min_date, args.max_date)
    analyzer.calculate_wmape(args.top, args.min_date, args.max_date)
    analyzer.save_wmape_csv('mapes.csv')
    analyzer.print_features(args.top)


if __name__ == "__main__":
    main()

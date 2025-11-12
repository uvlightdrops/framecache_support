import polars as pl

class SQLTableMixin:
    def read_sql(self, query: str, df=None):
        """
        Führt eine SQL-Abfrage auf dem DataFrame aus (polars SQLContext).
        df: DataFrame, optional (sonst self.df)
        query: SQL-String, z.B. "SELECT * FROM df WHERE col='val'"
        """
        if df is None:
            df = getattr(self, 'df', None)
        if df is None:
            raise ValueError('Kein DataFrame zum SQL-Query vorhanden.')
        # Polars SQLContext benötigt polars df
        polars_df = pl.from_pandas(df)
        args = {'df': polars_df}
        ctx = pl.SQLContext(args)
        tmp = ctx.execute(query)
        poldf = tmp.collect()
        return poldf.to_pandas()

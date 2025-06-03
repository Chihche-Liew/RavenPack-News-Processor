import re
import os
import wrds
import warnings
import pandas as pd
import polars as pl
import duckdb as db
from tqdm import tqdm
warnings.filterwarnings('ignore')


class RavenPackNewsProcessor:
    def __init__(
            self,
            keywords_path: str,
            start_year: int,
            end_year: int,
            output_dir: str | None = None,
            wrds_conn: wrds.Connection | None = None
    ):
        self.keywords_path = keywords_path
        self.start_year = start_year
        self.end_year = end_year
        self.output_dir = output_dir
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
        self.conn = wrds_conn
        self.rpna_company_names_df: pl.DataFrame | None = None
        self.comp_names_df: pl.DataFrame | None = None
        self.keywords: list[str] = []
        self._keyword_pattern: str | None = None
        self._fetch_linktables()
        self._load_keywords()

    def _fetch_ravenpack(self, year: int) -> pl.DataFrame:
        print(f"Fetching RavenPack data for {year} from WRDS...")
        try:
            query = f"""
                SELECT rp_story_id, rp_entity_id, entity_name, timestamp_utc, 
                       type, relevance, fact_level, news_type, headline, event_text, country_code
                FROM ravenpack_dj.rpa_djpr_equities_{year}
            """
            ravenpack_pd_df = self.conn.raw_sql(query, date_cols=['timestamp_utc'])
            ravenpack_pl_df = pl.from_pandas(ravenpack_pd_df)
            return ravenpack_pl_df
        except Exception as e:
            print(f"Error fetching RavenPack data for year {year} from WRDS: {e}")
            return pl.DataFrame()

    def _fetch_linktables(self) -> None:
        try:
            rpna_names_pd = self.conn.raw_sql(
                "SELECT rp_entity_id, cusip FROM rpna.wrds_company_names WHERE cusip IS NOT NULL AND cusip != ''"
            )
            if rpna_names_pd is None or rpna_names_pd.empty:
                raise ValueError("Failed to fetch or received empty data for rpna.wrds_company_names.")
            self.rpna_company_names_df = pl.from_pandas(rpna_names_pd)
            comp_names_pd = self.conn.raw_sql(
                "SELECT cusip, gvkey, tic FROM comp.names WHERE cusip IS NOT NULL AND cusip != '' AND gvkey IS NOT NULL"
            )
            if comp_names_pd is None or comp_names_pd.empty:
                raise ValueError("Failed to fetch or received empty data for comp.names.")
            self.comp_names_df = pl.from_pandas(comp_names_pd)
        except Exception as e:
            print(f"Error fetching link tables from WRDS: {e}")
            self.rpna_company_names_df = pl.DataFrame()
            self.comp_names_df = pl.DataFrame()
            raise

    def _preprocess_events(self, ravenpack_df: pl.DataFrame) -> pl.DataFrame:
        processed_df = ravenpack_df
        processed_df = processed_df.with_columns(
            pl.col('timestamp_utc')
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("US/Eastern")
            .dt.replace_time_zone(None)
            .alias('timestamp')
        )

        processed_df = processed_df.filter(
            (pl.col('country_code') == 'US') & (pl.col('relevance') >= 75)
        )

        processed_df = processed_df.with_columns(
            pl.col('headline').cast(pl.String).fill_null("")
            .str.to_lowercase()
            .str.replace_all(r'[^\w\s]', " ", literal=False)
            .str.replace_all(r'\s+', " ", literal=False)
            .str.strip_chars()
            .alias('headline')
        )

        processed_df = processed_df.sort(['rp_entity_id', 'timestamp'])
        return processed_df

    def _deduplicate_events(self, preprocessed_df: pl.DataFrame) -> pl.DataFrame:
        query = """
        WITH deduplicated_events AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY rp_entity_id, event_text) AS row_num
            FROM preprocessed_df 
        )
        SELECT rp_entity_id, entity_name, rp_story_id, timestamp, type,
               relevance, fact_level, news_type, headline, event_text
        FROM deduplicated_events WHERE row_num = 1;
        """
        deduplicated_df = db.query(query).pl()
        return deduplicated_df

    def _merge_identifiers(self, deduplicated_df: pl.DataFrame) -> pl.DataFrame:
        rpa_comp_names_link = self.rpna_company_names_df
        comp_names_link = self.comp_names_df

        query = """
        SELECT DISTINCT rpa.rp_entity_id, comp.cusip, comp.gvkey, comp.tic
        FROM rpa_comp_names_link AS rpa
        JOIN comp_names_link AS comp ON rpa.cusip = comp.cusip
        """

        link_table_df = db.query(query).pl()

        if link_table_df.is_empty():
            return deduplicated_df.with_columns([
                pl.lit(None).cast(pl.String).alias("cusip"),
                pl.lit(None).cast(pl.String).alias("gvkey"),
                pl.lit(None).cast(pl.String).alias("tic")
            ])

        merged_df = deduplicated_df.join(link_table_df, on='rp_entity_id', how='left')
        return merged_df

    def _load_keywords(self) -> None:
        with open(self.keywords_path, 'r', encoding='utf-8') as f:
            raw_keywords = [line.strip().lower() for line in f if line.strip()]

        keywords_patterns = []
        for kw in raw_keywords:
            if ' ' in kw:
                pattern = r'\b' + r'\s+'.join([re.escape(w) for w in kw.split()]) + r'\b'
            else:
                pattern = r'\b' + re.escape(kw) + r'\b'
            keywords_patterns.append(pattern)

        self.keywords = keywords_patterns
        self._keyword_pattern = '|'.join(self.keywords)

    def _filter_key_events(self, merged_df: pl.DataFrame) -> pl.DataFrame:
        filtered_df = merged_df.filter(
            pl.col('headline').cast(pl.String).fill_null("").str.contains(self._keyword_pattern)
        )
        return filtered_df

    def _process_ravenpack(self, year: int) -> pd.DataFrame:
        print(f"\n--- Processing RavenPack for year: {year} ---")
        raw_df = self._fetch_ravenpack(year)
        preprocessed_df = self._preprocess_events(raw_df)
        deduplicated_df = self._deduplicate_events(preprocessed_df)
        merged_df = self._merge_identifiers(deduplicated_df)
        final_filtered_df = self._filter_key_events(merged_df)
        final_pandas_df = final_filtered_df.to_pandas()

        if self.output_dir:
            if not final_pandas_df.empty:
                output_file_parquet = os.path.join(self.output_dir, f"ravenpack_dj_{year}_processed.parquet")
                try:
                    final_pandas_df.to_parquet(output_file_parquet, engine='pyarrow', index=False, compression='snappy')
                    print(f"Saved processed data for year {year} to: {output_file_parquet}")
                except Exception as e:
                    print(f"Error saving Pandas DataFrame to Parquet for year {year}: {e}")
            else:
                print(f"No data to save for year {year}.")

        return final_pandas_df

    def process_ravenpacks(self) -> list[pd.DataFrame]:
        all_years_data = []
        for year_val in tqdm(range(self.start_year, self.end_year + 1)):
            yearly_df = self._process_ravenpack(year_val)
            if not yearly_df.empty:
                all_years_data.append(yearly_df)
            else:
                print(f"No processed data returned for year {year_val}.")

        return all_years_data

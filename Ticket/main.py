import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import logging
import os
import tempfile
import duckdb
from scipy import stats
import matplotlib.pyplot as plt
import psutil
import gc
from concurrent.futures import ThreadPoolExecutor
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TicketLifecycleAnalyzer:
    def __init__(self, 
                data_directory: str,
                parquet_data_directory: str, 
                duckdb_file_path: Optional[str] = None,
                batch_size: int = 10000,
                file_batch_size: int = 20,
                max_workers: int = 2,
                memory_limit_gb: float = 6.0):
        """
        Initialize the ticket analyzer.
        Processes CSV and JSON files, converts to Parquet, then processes using DuckDB.
        """
        self.data_directory = Path(data_directory)
        self.parquet_data_directory = Path(parquet_data_directory)
        self.batch_size = batch_size
        self.file_batch_size = file_batch_size
        self.max_workers = max_workers
        self.memory_limit_gb = memory_limit_gb
        
        # Add temp file tracking
        self._temp_files_to_cleanup = []
        
        self.duckdb_conn = None
        self._db_initialized = False
        
        self._duckdb_file_path_param = duckdb_file_path
        self._actual_duckdb_file = None
        self._is_temp_db_file_managed = False

        self.parquet_data_directory.mkdir(parents=True, exist_ok=True)

    def _check_memory_usage(self):
        """Monitor memory usage and trigger cleanup if needed."""
        current_memory = psutil.virtual_memory().used / (1024**3)
        if current_memory > self.memory_limit_gb:
            logging.warning(f"Memory usage: {current_memory:.1f}GB - triggering cleanup")
            gc.collect()
        else:
            logging.info(f"Memory usage: {current_memory:.1f}GB")

    def __enter__(self):
        logging.info("Entering context, initializing DuckDB...")
        try:
            if self._duckdb_file_path_param:
                self._actual_duckdb_file = Path(self._duckdb_file_path_param)
                self._is_temp_db_file_managed = False
                logging.info(f"Using user-provided DuckDB database: {self._actual_duckdb_file}")
            else:
                fd, temp_db_filename = tempfile.mkstemp(suffix=".duckdb")
                os.close(fd) 
                self._actual_duckdb_file = Path(temp_db_filename)
                self._is_temp_db_file_managed = True
                logging.info(f"Using temporary DuckDB database: {self._actual_duckdb_file}")

            self.duckdb_conn = duckdb.connect(database=str(self._actual_duckdb_file.resolve()), read_only=False)
            logging.info(f"DuckDB connected to: {self._actual_duckdb_file.resolve()}")
            
            self._db_initialized = True 
            self._init_duckdb_tables()
            
            logging.info("DuckDB resources initialized successfully.")
            return self
        except Exception as e:
            logging.error(f"Failed to initialize DuckDB in __enter__: {e}", exc_info=True)
            if self.duckdb_conn:
                try: self.duckdb_conn.close()
                except Exception as ce: logging.error(f"Error closing DuckDB connection during __enter__ cleanup: {ce}")
            
            if self._is_temp_db_file_managed and self._actual_duckdb_file and self._actual_duckdb_file.exists():
                try: os.remove(self._actual_duckdb_file)
                except OSError as oe: logging.error(f"Error removing temporary DuckDB {self._actual_duckdb_file} during __enter__ cleanup: {oe}")
            
            self.duckdb_conn = None
            self._actual_duckdb_file = None
            self._db_initialized = False
            raise ConnectionError(f"Failed to initialize resources in __enter__: {e}") from e

    def _add_temp_file_for_cleanup(self, file_path: Path):
        """Track temporary files for cleanup."""
        self._temp_files_to_cleanup.append(file_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info("Exiting context, cleaning up DuckDB resources...")
        
        # Clean up tracked temporary files first
        for temp_file in self._temp_files_to_cleanup:
            try:
                if temp_file.exists():
                    temp_file.unlink()
                    logging.info(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                logging.warning(f"Could not clean up temp file {temp_file}: {e}")
        
        if self.duckdb_conn:
            try:
                self.duckdb_conn.close()
                logging.info(f"Closed DuckDB connection to {self._actual_duckdb_file}.")
            except Exception as e:
                logging.error(f"Error closing DuckDB connection in __exit__: {e}")
        
        if self._is_temp_db_file_managed and self._actual_duckdb_file and self._actual_duckdb_file.exists():
            try:
                os.remove(self._actual_duckdb_file)
                logging.info(f"Removed temporary DuckDB database: {self._actual_duckdb_file}")
            except OSError as e:
                logging.error(f"Error removing temporary DuckDB database {self._actual_duckdb_file} in __exit__: {e}")
        
        self.duckdb_conn = None
        self._actual_duckdb_file = None
        self._db_initialized = False
        self._is_temp_db_file_managed = False
        self._temp_files_to_cleanup = []
        logging.info("DuckDB resources cleaned up.")
        return False

    def _ensure_db_initialized(self):
        if not self._db_initialized or not self.duckdb_conn:
            msg = "DuckDB not initialized. Ensure the TicketLifecycleAnalyzer is used within a 'with' statement."
            logging.error(msg)
            raise ConnectionError(msg)
    
    def _init_duckdb_tables(self):
        self._ensure_db_initialized()
        try:
            # Create main table
            self.duckdb_conn.execute("""
                CREATE TABLE IF NOT EXISTS ticket_snapshots (
                    issueId TEXT,
                    customerId TEXT,
                    lastModified TEXT,
                    ticket_id TEXT,
                    created_on TIMESTAMPTZ, 
                    last_updated TIMESTAMPTZ,
                    _ts TIMESTAMPTZ,
                    snapshot_date TEXT
                )
            """)
            
            # Create temporary staging table for direct file processing
            self.duckdb_conn.execute("""
                CREATE TABLE IF NOT EXISTS temp_staging (
                    issueId TEXT,
                    customerId TEXT,
                    lastModified TEXT,
                    id TEXT,
                    createdTimeUtc TEXT,
                    lastModifiedTimeUtc TEXT,
                    _ts BIGINT,
                    snapshot_date TEXT
                )
            """)
            
            self.duckdb_conn.execute("CREATE INDEX IF NOT EXISTS idx_ticket_id ON ticket_snapshots(ticket_id)")
            self.duckdb_conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON ticket_snapshots(_ts)")
            logging.info("DuckDB tables and indexes prepared.")
        except Exception as e:
            logging.error(f"DuckDB error during table initialization: {e}")
            raise

    def _process_json_file(self, json_file_path: Path) -> bool:
        """Process JSON file using DuckDB's native JSON reader."""
        try:
            snapshot_date_str = json_file_path.stem
            file_path_str = str(json_file_path.resolve()).replace('\\', '/')
            
            # Clear staging table
            self.duckdb_conn.execute("DELETE FROM temp_staging")
            
            # Try to read JSON file directly with DuckDB
            try:
                # First, try to read the JSON structure to understand it
                sample_query = f"SELECT * FROM read_json_auto('{file_path_str}') LIMIT 1"
                sample_result = self.duckdb_conn.execute(sample_query).fetchdf()
                
                if sample_result.empty:
                    logging.warning(f"JSON file {json_file_path.name} appears to be empty.")
                    return False
                
                # Load data into staging table with snapshot_date
                insert_query = f"""
                    INSERT INTO temp_staging 
                    SELECT *, '{snapshot_date_str}' as snapshot_date 
                    FROM read_json_auto('{file_path_str}')
                """
                self.duckdb_conn.execute(insert_query)
                
                # Transform and insert into main table
                self._transform_staging_to_main()
                
                logging.info(f"Successfully processed JSON file: {json_file_path.name}")
                return True
                
            except Exception as e:
                logging.warning(f"DuckDB JSON auto-reader failed for {json_file_path.name}: {e}")
                # Fallback to pandas processing
                return self._process_json_file_pandas(json_file_path)
                
        except Exception as e:
            logging.error(f"Error processing JSON file {json_file_path.name}: {e}")
            return False

    def _process_json_file_pandas(self, json_file_path: Path) -> bool:
        """Fallback method to process JSON using pandas."""
        try:
            snapshot_date_str = json_file_path.stem
            
            # Try different JSON reading approaches
            try:
                # Try reading as JSON lines
                df = pd.read_json(json_file_path, lines=True)
            except:
                try:
                    # Try reading as regular JSON
                    df = pd.read_json(json_file_path)
                except:
                    # Try reading as text and parsing manually
                    with open(json_file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    df = pd.json_normalize(data)
            
            if df.empty:
                logging.warning(f"JSON file {json_file_path.name} resulted in empty DataFrame.")
                return False
            
            df['snapshot_date'] = snapshot_date_str
            df_processed = self._validate_snapshot_dataframe(df)
            
            # Save to temporary parquet for processing
            temp_parquet = self.parquet_data_directory / f"temp_{json_file_path.stem}.parquet"
            df_processed.to_parquet(temp_parquet, index=False)
            self._add_temp_file_for_cleanup(temp_parquet)
            
            # Load into DuckDB
            file_path_str = str(temp_parquet.resolve()).replace('\\', '/')
            self.duckdb_conn.execute(f"INSERT INTO ticket_snapshots SELECT * FROM read_parquet('{file_path_str}')")
            
            # Clean up temporary file
            try:
                temp_parquet.unlink()
                self._temp_files_to_cleanup.remove(temp_parquet)
            except Exception as e:
                logging.warning(f"Could not clean up temp file {temp_parquet}: {e}")
            
            logging.info(f"Successfully processed JSON file via pandas: {json_file_path.name}")
            return True
            
        except Exception as e:
            logging.error(f"Error processing JSON file {json_file_path.name} with pandas: {e}")
            return False

    def _process_csv_file_efficient(self, csv_file_path: Path) -> bool:
        """Process CSV file using DuckDB's native CSV reader when possible."""
        try:
            snapshot_date_str = csv_file_path.stem
            file_path_str = str(csv_file_path.resolve()).replace('\\', '/')
            
            # Clear staging table
            self.duckdb_conn.execute("DELETE FROM temp_staging")
            
            try:
                # Try DuckDB's native CSV reader first
                insert_query = f"""
                    INSERT INTO temp_staging 
                    SELECT *, '{snapshot_date_str}' as snapshot_date 
                    FROM read_csv_auto('{file_path_str}')
                """
                self.duckdb_conn.execute(insert_query)
                
                # Transform and insert into main table
                self._transform_staging_to_main()
                
                logging.info(f"Successfully processed CSV file with DuckDB: {csv_file_path.name}")
                return True
                
            except Exception as e:
                logging.warning(f"DuckDB CSV auto-reader failed for {csv_file_path.name}: {e}")
                # Fallback to pandas chunk processing
                return self._process_csv_file_pandas(csv_file_path)
                
        except Exception as e:
            logging.error(f"Error processing CSV file {csv_file_path.name}: {e}")
            return False

    def _process_csv_file_pandas(self, csv_file_path: Path) -> bool:
        """Fallback method to process CSV using pandas with chunking."""
        try:
            snapshot_date_str = csv_file_path.stem
            
            # Process in chunks to avoid memory issues
            for chunk_idx, df_chunk in enumerate(pd.read_csv(csv_file_path, chunksize=self.batch_size, encoding='utf-8', low_memory=False)):
                if df_chunk.empty:
                    continue
                
                df_validated = self._validate_snapshot_dataframe(df_chunk.copy())
                df_validated['snapshot_date'] = snapshot_date_str
                
                # Save chunk to temporary parquet
                temp_parquet = self.parquet_data_directory / f"temp_{csv_file_path.stem}_chunk_{chunk_idx}.parquet"
                df_validated.to_parquet(temp_parquet, index=False)
                
                # Load into DuckDB
                file_path_str = str(temp_parquet.resolve()).replace('\\', '/')
                self.duckdb_conn.execute(f"INSERT INTO ticket_snapshots SELECT * FROM read_parquet('{file_path_str}')")
                
                # Clean up temporary file
                temp_parquet.unlink()
                
                # Check memory usage periodically
                if chunk_idx % 10 == 0:
                    self._check_memory_usage()
            
            logging.info(f"Successfully processed CSV file with pandas: {csv_file_path.name}")
            return True
            
        except Exception as e:
            logging.error(f"Error processing CSV file {csv_file_path.name} with pandas: {e}")
            return False

    def _transform_staging_to_main(self):
        """Transform data from staging table to main table with proper column mapping."""
        transform_query = """
            INSERT INTO ticket_snapshots
            SELECT 
                issueId,
                customerId,
                lastModified,
                COALESCE(id, '') as ticket_id,
                TRY_CAST(createdTimeUtc AS TIMESTAMPTZ) as created_on,
                TRY_CAST(lastModifiedTimeUtc AS TIMESTAMPTZ) as last_updated,
                CASE 
                    WHEN _ts IS NOT NULL THEN to_timestamp(_ts)
                    ELSE NULL 
                END as _ts,
                snapshot_date
            FROM temp_staging
        """
        self.duckdb_conn.execute(transform_query)

    def _process_single_file(self, file_path: Path) -> bool:
        """Process a single file (CSV or JSON) efficiently."""
        file_extension = file_path.suffix.lower()
        
        if file_extension == '.json':
            return self._process_json_file(file_path)
        elif file_extension == '.csv':
            return self._process_csv_file_efficient(file_path)
        else:
            logging.warning(f"Unsupported file type: {file_path.name}")
            return False

    def convert_files_to_parquet(self, force_conversion: bool = False, use_parallel: bool = True):
        """
        Convert CSV and JSON files to Parquet format with memory-efficient processing.
        
        Args:
            force_conversion: Force re-conversion of existing parquet files
            use_parallel: Use parallel processing for file conversion
        """
        logging.info(f"Starting file conversion to Parquet. Output directory: {self.parquet_data_directory}")
        
        # Get all supported files
        json_files = sorted(list(self.data_directory.glob("*.json")))
        csv_files = sorted(list(self.data_directory.glob("*.csv")))
        all_files = json_files + csv_files
        
        if not all_files:
            logging.warning(f"No CSV or JSON files found in {self.data_directory}")
            return

        logging.info(f"Found {len(csv_files)} CSV files and {len(json_files)} JSON files")
        
        # Filter files that need processing
        files_to_process = []
        for file_path in all_files:
            parquet_file_name = file_path.stem + ".parquet"
            parquet_file_path = self.parquet_data_directory / parquet_file_name
            
            if force_conversion or not parquet_file_path.exists():
                files_to_process.append(file_path)
            else:
                logging.info(f"Parquet file {parquet_file_path.name} already exists. Skipping.")
        
        if not files_to_process:
            logging.info("No files need processing.")
            return
        
        # Process files in batches
        for i in range(0, len(files_to_process), self.file_batch_size):
            batch = files_to_process[i:i + self.file_batch_size]
            logging.info(f"Processing batch {i//self.file_batch_size + 1}: {len(batch)} files")
            
            if use_parallel and len(batch) > 1:
                self._process_file_batch_parallel(batch)
            else:
                self._process_file_batch_sequential(batch)
            
            # Check memory usage after each batch
            self._check_memory_usage()
        
        logging.info("File conversion completed.")

    def _process_file_batch_parallel(self, file_batch: List[Path]):
        """Process a batch of files in parallel."""
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(file_batch))) as executor:
            futures = [executor.submit(self._convert_single_file_to_parquet, file_path) for file_path in file_batch]
            
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in parallel file processing: {e}")

    def _process_file_batch_sequential(self, file_batch: List[Path]):
        """Process a batch of files sequentially."""
        for file_path in file_batch:
            self._convert_single_file_to_parquet(file_path)

    def _convert_single_file_to_parquet(self, file_path: Path):
        """Convert a single file to Parquet format."""
        try:
            parquet_file_name = file_path.stem + ".parquet"
            parquet_file_path = self.parquet_data_directory / parquet_file_name
            
            logging.info(f"Converting {file_path.name} -> {parquet_file_name}")
            
            if file_path.suffix.lower() == '.json':
                success = self._convert_json_to_parquet(file_path, parquet_file_path)
            else:
                success = self._convert_csv_to_parquet(file_path, parquet_file_path)
            
            if success:
                logging.info(f"Successfully converted {file_path.name}")
            else:
                logging.error(f"Failed to convert {file_path.name}")
                
        except Exception as e:
            logging.error(f"Error converting {file_path.name}: {e}")

    def _convert_json_to_parquet(self, json_file_path: Path, parquet_file_path: Path) -> bool:
        """Convert JSON file to Parquet."""
        try:
            snapshot_date_str = json_file_path.stem
            
            # Try different JSON reading approaches
            df = None
            try:
                df = pd.read_json(json_file_path, lines=True)
            except:
                try:
                    df = pd.read_json(json_file_path)
                except:
                    with open(json_file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    df = pd.json_normalize(data)
            
            if df is None or df.empty:
                logging.warning(f"Could not read JSON file or file is empty: {json_file_path.name}")
                return False
            
            df['snapshot_date'] = snapshot_date_str
            df_processed = self._validate_snapshot_dataframe(df)
            
            # Save to parquet
            columns_to_keep = ['issueId', 'customerId', 'lastModified', 
                              'ticket_id', 'created_on', 'last_updated', 
                              '_ts', 'snapshot_date']
            
            for col in columns_to_keep:
                if col not in df_processed.columns:
                    if col in ['created_on', 'last_updated', '_ts']:
                        df_processed[col] = pd.NaT
                    else:
                        df_processed[col] = None
            
            df_to_save = df_processed[columns_to_keep]
            df_to_save.to_parquet(parquet_file_path, index=False)
            
            return True
            
        except Exception as e:
            logging.error(f"Error converting JSON to Parquet {json_file_path.name}: {e}")
            return False

    def _convert_csv_to_parquet(self, csv_file_path: Path, parquet_file_path: Path) -> bool:
        """Convert CSV file to Parquet using streaming approach."""
        try:
            snapshot_date_str = csv_file_path.stem
            
            # Process chunks directly to parquet files, then combine
            temp_parquet_files = []
            
            for chunk_idx, df_chunk in enumerate(pd.read_csv(csv_file_path, chunksize=self.batch_size, encoding='utf-8', low_memory=False)):
                if df_chunk.empty:
                    continue
                    
                df_validated = self._validate_snapshot_dataframe(df_chunk.copy())
                df_validated['snapshot_date'] = snapshot_date_str
                
                # Save each chunk as a separate parquet file
                temp_parquet_chunk = self.parquet_data_directory / f"temp_chunk_{chunk_idx}_{csv_file_path.stem}.parquet"
                df_validated.to_parquet(temp_parquet_chunk, index=False)
                temp_parquet_files.append(temp_parquet_chunk)
                self._add_temp_file_for_cleanup(temp_parquet_chunk)
            
            # Use DuckDB to combine all chunks efficiently
            if temp_parquet_files:
                file_paths = [str(f.resolve()).replace('\\', '/') for f in temp_parquet_files]
                parquet_path = str(parquet_file_path.resolve()).replace('\\', '/')
                
                # Use DuckDB to combine all chunks into final parquet
                files_list = "', '".join(file_paths)
                self.duckdb_conn.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet(['{files_list}'])
                    ) TO '{parquet_path}' (FORMAT PARQUET)
                """)
                
                # Clean up temporary files
                for temp_file in temp_parquet_files:
                    try:
                        temp_file.unlink()
                        self._temp_files_to_cleanup.remove(temp_file)
                    except Exception as e:
                        logging.warning(f"Could not clean up temp file {temp_file}: {e}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error converting CSV to Parquet {csv_file_path.name}: {e}")
            return False

    def _validate_snapshot_dataframe(self, df_chunk: pd.DataFrame) -> pd.DataFrame:
        """Validate and standardize snapshot dataframe columns."""
        column_map = {
            'id': 'ticket_id',
            'createdTimeUtc': 'created_on',
            'lastModifiedTimeUtc': 'last_updated',
        }
        df_chunk.rename(columns=column_map, inplace=True)

        if '_ts' not in df_chunk.columns: 
            df_chunk['_ts'] = pd.NA
        if 'ticket_id' not in df_chunk.columns: 
            df_chunk['ticket_id'] = None
        if 'created_on' not in df_chunk.columns: 
            df_chunk['created_on'] = pd.NaT
        if 'last_updated' not in df_chunk.columns: 
            df_chunk['last_updated'] = pd.NaT
            
        if '_ts' in df_chunk.columns:
            df_chunk['_ts'] = pd.to_numeric(df_chunk['_ts'], errors='coerce')
            df_chunk['_ts'] = pd.to_datetime(df_chunk['_ts'], unit='s', utc=True, errors='coerce')

        for col in ['created_on', 'last_updated']:
            if col in df_chunk.columns:
                df_chunk[col] = pd.to_datetime(df_chunk[col], utc=True, errors='coerce')
        
        if 'last_updated' in df_chunk.columns and '_ts' in df_chunk.columns:
            condition = df_chunk['last_updated'].isnull() & df_chunk['_ts'].notnull()
            df_chunk.loc[condition, 'last_updated'] = df_chunk.loc[condition, '_ts']
        
        return df_chunk

    def process_snapshots_direct(self) -> Dict:
        """Process files directly into DuckDB without intermediate Parquet files."""
        self._ensure_db_initialized()
        
        # Get all supported files
        json_files = sorted(list(self.data_directory.glob("*.json")))
        csv_files = sorted(list(self.data_directory.glob("*.csv")))
        all_files = json_files + csv_files
        
        if not all_files:
            logging.warning(f"No CSV or JSON files found in {self.data_directory}")
            return self._get_empty_results_structure()

        logging.info(f"Processing {len(all_files)} files directly into DuckDB")
        
        # Process files in batches
        processed_count = 0
        for i in range(0, len(all_files), self.file_batch_size):
            batch = all_files[i:i + self.file_batch_size]
            logging.info(f"Processing batch {i//self.file_batch_size + 1}: {len(batch)} files")
            
            for file_path in batch:
                if self._process_single_file(file_path):
                    processed_count += 1
                
                # Check memory usage periodically
                if processed_count % 10 == 0:
                    self._check_memory_usage()
        
        logging.info(f"Processed {processed_count} files successfully")
        return self._calculate_metrics_from_duckdb()

    def process_snapshots(self) -> Dict:
        """Process snapshots using existing Parquet files."""
        self._ensure_db_initialized()
        parquet_files = sorted(list(self.parquet_data_directory.glob("*.parquet")))
        
        if not parquet_files:
            logging.warning(f"No Parquet files found in {self.parquet_data_directory}")
            return self._get_empty_results_structure()

        logging.info(f"Processing {len(parquet_files)} Parquet files")
        
        # Process files in batches
        for i in range(0, len(parquet_files), self.file_batch_size):
            batch = parquet_files[i:i + self.file_batch_size]
            logging.info(f"Processing Parquet batch {i//self.file_batch_size + 1}: {len(batch)} files")
            
            for file_path in batch:
                try:
                    file_path_str = str(file_path.resolve()).replace('\\', '/')
                    self.duckdb_conn.execute(f"INSERT INTO ticket_snapshots SELECT * FROM read_parquet('{file_path_str}')")
                    logging.info(f"Processed: {file_path.name}")
                except Exception as e:
                    logging.error(f"Error processing {file_path.name}: {e}")
                    continue
            
            # Check memory usage after each batch
            self._check_memory_usage()
        
        return self._calculate_metrics_from_duckdb()

    def _get_empty_results_structure(self) -> Dict:
        return {
            'average_lifetime_system': 0, 'max_lifetime_system': 0,
            'lifetime_distribution': (np.array([]), np.array([])),
            'update_frequency_distribution': (np.array([]), np.array([])),
            'system_user_update_diff': {
                'mean': 0, 'std': 0, 'distribution': (np.array([]), np.array([]))
            },
            'cutoff_point': 0
        }

    def _calculate_metrics_from_duckdb(self) -> Dict:
        self._ensure_db_initialized()
        logging.info("Calculating metrics from DuckDB database...")

        lifetime_query = """
        WITH ticket_first_last AS (
            SELECT
                ticket_id,
                MIN(COALESCE(created_on, _ts)) as first_seen,
                MAX(_ts) as last_seen_ts,
                COUNT(*) as snapshot_count
            FROM ticket_snapshots
            WHERE ticket_id IS NOT NULL 
            AND ticket_id != '' 
            AND _ts IS NOT NULL
            AND ticket_id NOT LIKE '%test%'  -- Filter out test tickets
            GROUP BY ticket_id
            HAVING COUNT(*) > 1  -- Need at least 2 snapshots for meaningful analysis
        )
        SELECT
            ticket_id,
            first_seen,
            last_seen_ts,
            snapshot_count,
            EXTRACT(EPOCH FROM (last_seen_ts - first_seen)) as lifetime_seconds
        FROM ticket_first_last
        WHERE EXTRACT(EPOCH FROM (last_seen_ts - first_seen)) > 0
        AND EXTRACT(EPOCH FROM (last_seen_ts - first_seen)) < 31536000  -- Less than 1 year (filter outliers)
        """
        try:
            df_lifetimes = self.duckdb_conn.execute(lifetime_query).fetchdf()
            lifetimes_seconds = df_lifetimes['lifetime_seconds'].tolist() if not df_lifetimes.empty else []
        except Exception as e: 
            logging.error(f"Error calculating lifetimes from DuckDB: {e}", exc_info=True)
            return self._get_empty_results_structure()

        intervals_query = """
        WITH ordered_snapshots AS (
            SELECT
                ticket_id,
                _ts,
                LAG(_ts, 1) OVER (PARTITION BY ticket_id ORDER BY _ts) as prev_ts
            FROM ticket_snapshots
            WHERE ticket_id IS NOT NULL AND ticket_id != '' AND _ts IS NOT NULL
        )
        SELECT
            EXTRACT(EPOCH FROM (_ts - prev_ts)) as interval_seconds
        FROM ordered_snapshots
        WHERE prev_ts IS NOT NULL AND EXTRACT(EPOCH FROM (_ts - prev_ts)) >= 0
        """
        try:
            df_intervals = self.duckdb_conn.execute(intervals_query).fetchdf()
            all_intervals = df_intervals['interval_seconds'].tolist() if not df_intervals.empty else []
        except Exception as e:
            logging.error(f"Error calculating update intervals from DuckDB: {e}", exc_info=True)
            all_intervals = [] 

        diffs_query = """
        SELECT
            EXTRACT(EPOCH FROM (_ts - last_updated)) as diff_seconds
        FROM ticket_snapshots
        WHERE ticket_id IS NOT NULL AND ticket_id != ''
          AND _ts IS NOT NULL AND last_updated IS NOT NULL
        """
        try:
            df_diffs = self.duckdb_conn.execute(diffs_query).fetchdf()
            all_diffs = df_diffs['diff_seconds'].tolist() if not df_diffs.empty else []
        except Exception as e:
            logging.error(f"Error calculating system-user update diffs from DuckDB: {e}", exc_info=True)
            all_diffs = [] 

        logging.info("Metrics calculation from DuckDB finished.")
        
        return {
            'average_lifetime_system': np.mean(lifetimes_seconds) if lifetimes_seconds else 0,
            'max_lifetime_system': np.max(lifetimes_seconds) if lifetimes_seconds else 0,
            'lifetime_distribution': np.histogram(lifetimes_seconds, bins=50) if lifetimes_seconds else (np.array([]), np.array([])),
            'update_frequency_distribution': np.histogram(all_intervals, bins=50) if all_intervals else (np.array([]), np.array([])),
            'system_user_update_diff': {
                'mean': np.mean(all_diffs) if all_diffs else 0,
                'std': np.std(all_diffs) if all_diffs else 0,
                'distribution': np.histogram(all_diffs, bins=50) if all_diffs else (np.array([]), np.array([]))
            },
            'cutoff_point': np.percentile(lifetimes_seconds, 95) if lifetimes_seconds and len(lifetimes_seconds) > 0 else 0
        }

    def plot_distributions(self, results: Dict, output_dir: str = 'plots'):
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Saving plots to {output_path.resolve()}")

        plt.figure(figsize=(12, 7))
        if results['lifetime_distribution'] and results['lifetime_distribution'][0].size > 0:
            counts, bins = results['lifetime_distribution']
            if bins.size > 1: 
                bins_days = bins / 86400.0 
                bin_centers_days = (bins_days[:-1] + bins_days[1:]) / 2.0
                bin_width_days = (bins_days[1]-bins_days[0]) if len(bins_days)>1 else 1.0
                plt.bar(bin_centers_days, counts, width=bin_width_days*0.9, alpha=0.6, label='Lifetime Distribution')
                
                if results['average_lifetime_system'] > 0 and sum(counts) > 1:
                    kde_sample_data_seconds = np.repeat((bins[:-1] + bins[1:]) / 2.0, counts.astype(int))
                    if len(kde_sample_data_seconds) > 1 : 
                        kde_sample_data_days = kde_sample_data_seconds / 86400.0
                        try:
                            kde = stats.gaussian_kde(kde_sample_data_days)
                            x_range_days = np.linspace(min(bins_days), max(bins_days), 500)
                            kde_values = kde(x_range_days)
                            if np.max(kde_values) > 0: 
                                plt.plot(x_range_days, kde_values * np.max(counts) / np.max(kde_values), 'r-', label='KDE (approx.)')
                        except Exception as e: logging.warning(f"Could not generate KDE for lifetime: {e}")

                mean_lifetime_days = results['average_lifetime_system'] / 86400.0
                plt.axvline(mean_lifetime_days, color='g', linestyle='dashed', linewidth=1.5, label=f'Mean: {mean_lifetime_days:.2f} days')
                
                if counts.size > 0:
                    cumsum_counts = np.cumsum(counts)
                    if cumsum_counts[-1] > 0: 
                        median_bin_index = np.searchsorted(cumsum_counts, cumsum_counts[-1] / 2.0)
                        if median_bin_index < len(bins_days) -1 : 
                            median_lifetime_days = bins_days[median_bin_index] 
                            plt.axvline(median_lifetime_days, color='b', linestyle='dashed', linewidth=1.5, label=f'Median: {median_lifetime_days:.2f} days')
                
                cutoff_point_days = results.get('cutoff_point', 0) / 86400.0
                if cutoff_point_days > 0: plt.axvline(cutoff_point_days, color='purple', linestyle='dashed', linewidth=1.5, label=f'95th Percentile: {cutoff_point_days:.2f} days')
                
                plt.title('Ticket Lifetime Distribution'); plt.xlabel('Lifetime (days)'); plt.ylabel('Frequency (Number of Tickets)')
                plt.legend(); plt.grid(True, linestyle='--', alpha=0.7); plt.tight_layout()
                plt.savefig(output_path / 'lifetime_distribution_enhanced.png'); plt.close()
            else:
                logging.warning("Not enough data points in lifetime distribution bins to plot.")
        else: 
            logging.warning("No data for lifetime distribution plot.")


def print_results(results: Dict):
    processing_type = "Enhanced DuckDB Processing (CSV/JSON -> Direct or Parquet)"
    print(f"\n--- Analysis Results ({processing_type}) ---")
    if not results or 'average_lifetime_system' not in results:
        print("No valid results to display or results structure is unexpected.")
        return

    print(f"Average System Lifetime: {results.get('average_lifetime_system', 0) / 86400:.2f} days")
    print(f"Maximum System Lifetime: {results.get('max_lifetime_system', 0) / 86400:.2f} days")
    print(f"95th Percentile Lifetime (Cutoff Point): {results.get('cutoff_point', 0) / 86400:.2f} days")
    
    sys_user_diff = results.get('system_user_update_diff', {})
    if sys_user_diff:
        print(f"\nSystem vs User Update Difference:")
        print(f"  Mean difference: {sys_user_diff.get('mean', 0):.2f} seconds")
        print(f"  Std deviation: {sys_user_diff.get('std', 0):.2f} seconds")
    else:
        print("\nSystem vs User Update Difference data not available or structure is empty.")
    
    lifetime_dist_counts, lifetime_bins = results.get('lifetime_distribution', (np.array([]), np.array([])))
    if lifetime_dist_counts.size > 0:
        print(f"\nLifetime Distribution: {len(lifetime_bins)-1 if len(lifetime_bins)>0 else 0} bins, {np.sum(lifetime_dist_counts)} tickets")
    
    update_freq_counts, update_freq_bins = results.get('update_frequency_distribution', (np.array([]), np.array([])))
    if update_freq_counts.size > 0:
        print(f"Update Frequency Distribution: {len(update_freq_bins)-1 if len(update_freq_bins)>0 else 0} bins, {np.sum(update_freq_counts)} intervals")


def main():
    base_dir = Path(".") 
    data_dir = base_dir / "test_data"  # Changed to support both CSV and JSON
    parquet_dir = base_dir / "test_data_parquet"
    plot_dir = base_dir / "plot_results"

    # Use a persistent DuckDB file
    duckdb_persistent_file = base_dir / "ticket_analysis.duckdb"
    duckdb_path_to_use = str(duckdb_persistent_file)
    
    logging.info(f"Enhanced analyzer will use DuckDB file: {duckdb_path_to_use}")
    logging.info(f"Memory limit set to: 6.0 GB")
    logging.info(f"File batch size: 20 files per batch")
    logging.info(f"Parallel processing: enabled with 2 workers")

    try:
        with TicketLifecycleAnalyzer(
            data_directory=str(data_dir),
            parquet_data_directory=str(parquet_dir),
            duckdb_file_path=duckdb_path_to_use,
            batch_size=100,
            file_batch_size=20,
            max_workers=2,
            memory_limit_gb=6.0
        ) as analyzer:
            
            # Option 1: Convert to Parquet first, then process
            analyzer.convert_files_to_parquet(force_conversion=True, use_parallel=True)
            results = analyzer.process_snapshots()
            
            # Option 2: Process files directly (uncomment to use instead)
            # results = analyzer.process_snapshots_direct()
            
            print_results(results)
            analyzer.plot_distributions(results, output_dir=str(plot_dir))
        
        logging.info(f"Enhanced analysis complete. DuckDB file: {duckdb_path_to_use}")

    except ConnectionError as e:
        logging.error(f"Could not run analysis due to connection error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()

# Ticket Lifecycle Analyzer with DuckDB and Parquet

This Python script analyzes ticket data from CSV files to calculate various lifecycle metrics and generate insightful visualizations. It first preprocesses CSVs into the efficient Parquet format, then loads this data into a file-backed or temporary DuckDB database for robust analysis of potentially large datasets. It computes statistics such as average ticket lifetime, update frequency, and differences between system and user update times.

## Features

*   **CSV to Parquet Preprocessing**: Ingests ticket data from CSV files (processed in configurable batches) and converts them to the efficient Parquet format.
*   **DuckDB Backend**: Uses a DuckDB database for robust storage and querying.
    *   Can be configured as **file-backed** for persistence and handling datasets larger than memory.
    *   Can use a **temporary file** (auto-deleted) for smaller datasets or when persistence is not required.
*   **Comprehensive Metrics**: Calculates:
    *   Average and maximum ticket lifetimes.
    *   95th percentile lifetime (cutoff point).
    *   Distribution of ticket lifetimes.
    *   Distribution of update intervals between snapshots of the same ticket.
    *   Mean, standard deviation, and distribution of time differences between system snapshot timestamps (`_ts`) and user last modification timestamps (`lastModifiedTimeUtc`).
*   **Data Validation & Normalization**: Standardizes column names and converts timestamp fields to consistent UTC datetime objects during the CSV to Parquet conversion.
*   **Visualization**: Generates and saves plots for:
    *   Ticket Lifetime Distribution (with KDE).

## Prerequisites

*   Python 3.7+

## Setup

1.  **Download the Script**:
    Save the Python script (e.g., `ticket_analyzer.py`) to your local machine.

2.  **Prepare Data Directories**:
    *   **CSV Input Directory**: Create a directory where your input CSV files will be located. For example, `my_ticket_data_csv/`.
    *   **Parquet Output/Intermediate Directory**: The script will create a directory to store the Parquet files generated from the CSVs (e.g., `my_ticket_data_parquet/`). You can configure this path.
    *   The script's `main()` function currently defaults to looking for/creating `test_data_csv` and `test_data_parquet` directories for sample data. You'll need to modify these paths in the `main()` function to point to your actual data locations.

3.  **Install Dependencies**:
    Navigate to the directory where you saved the script and create a `requirements.txt` file with the content below. Then, install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

## Data Preparation (Input CSVs)

*   **File Format**: Initial input data must be in CSV format.
*   **Filename Convention**: The script extracts a `snapshot_date` from the CSV filename stem (e.g., `20230101.csv` results in `snapshot_date` '20230101'). This date is included in the Parquet files and subsequently in the DuckDB table.
*   **CSV Columns**: Each CSV file should contain the following columns:
    ```csv
    _ts,issueId,id,customerId,lastModified,createdTimeUtc,lastModifiedTimeUtc
    1706008231,CONR-4606,0ac006d0-75ee-4b2d-a61f-39f178205f2a,customer--5e38a375-3d50-4047-a19b-56784473924b,2024-01-23T11:10:30.9114993Z,2023-08-18T08:55:26.7053884Z,2024-01-23T11:10:30.9114993Z
    1725440733,TB-6850,4845912d-33a6-43aa-9b77-6df7c4a84256,customer--0f1235f0-04d6-4ea4-894a-162b7907cf31,2024-09-04T09:05:33.072088Z,2023-10-30T09:31:30.9409366Z,2024-09-04T09:05:33.072088Z
    ```

## Usage

1.  **Configure Paths in `main()`**:
    Open the script (`ticket_analyzer.py`) and modify the path variables within the `main()` function:
    ```python
    # In the main() function:
    base_dir = Path(".") 
    csv_dir = base_dir / "your_csv_data_directory"           # <--- CHANGE THIS
    parquet_dir = base_dir / "your_parquet_output_directory"  # <--- Optionally change this
    plot_dir = base_dir / "your_plots_output_directory"     # <--- Optionally change this

    # --- Configure DuckDB Database File ---
    # Option 1: Use a persistent DuckDB file (recommended for large data / re-analysis)
    # The file will be created if it doesn't exist.
    duckdb_persistent_file = base_dir / "my_ticket_analysis.duckdb"
    duckdb_path_to_use = str(duckdb_persistent_file)
    
    # Option 2: Use a temporary DuckDB file (will be auto-deleted on script exit)
    # duckdb_path_to_use = None 
    # --- End DuckDB Configuration ---

    with TicketLifecycleAnalyzer( csv_data_directory=str(csv_dir), parquet_data_directory=str(parquet_dir), duckdb_file_path=duckdb_path_to_use, batch_size=3)  as analyzer:
        
        analyzer.convert_csv_to_parquet(force_conversion=True) 
        results = analyzer.process_snapshots()
    
        print_results(results)
        analyzer.plot_distributions(results, output_dir=str(plot_dir))
    ```

2.  **Run the Script**:
    Execute the script from your terminal:
    ```bash
    python ticket_analyzer.py
    ```
    The script will:
    1.  Convert CSV files from `csv_dir` to Parquet files in `parquet_dir` (if not already present or `force_conversion=True`).
    2.  Load data from Parquet files into the DuckDB database.
    3.  Calculate metrics.
    4.  Generate plots.

## Output

*   **Console Output**: The script will print summary statistics to the console, including:
    *   Average and maximum system lifetimes.
    *   95th percentile lifetime.
    *   Update frequency and system-user update difference statistics.
*   **Plots**: Visualizations will be saved as PNG files in the directory specified by `plot_dir` (e.g., `plots_duckdb_file_backed` by default in the provided `main()` example).
*   **Parquet Files**: Intermediate Parquet files will be stored in the `parquet_dir`. These can be reused by subsequent runs if `force_conversion` is `False`.
*   **DuckDB Database File (Optional)**: If `duckdb_path_to_use` is set to a file path (not `None`), the DuckDB database file (e.g., `my_ticket_analysis.duckdb`) will persist after the script execution. This allows for re-analysis or external querying of the processed data without re-ingesting CSVs.

## Customization

*   **CSV Column Mapping**: If your CSV column names differ, adjust the `column_map` dictionary within the `_validate_snapshot_dataframe` method.
*   **Batch Size**: The `batch_size` for CSV reading during Parquet conversion can be changed when instantiating `TicketLifecycleAnalyzer`.
*   **Parquet Directory**: The `parquet_data_directory` can be configured.
*   **DuckDB Database File**: Choose between a persistent file path or `None` for a temporary database via the `duckdb_file_path` parameter.
*   **Plotting Parameters**: Histogram bins, plot titles, etc., can be adjusted within the `plot_distributions` method.

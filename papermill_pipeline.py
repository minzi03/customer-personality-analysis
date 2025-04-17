# pip install papermill

import papermill as pm
import os
from datetime import datetime

# Thư mục chứa notebook gốc
SOURCE_DIR = "src"

# Thư mục lưu notebook sau khi đã chạy
OUTPUT_DIR = "notebooks"

# Danh sách notebook cần chạy
NOTEBOOKS = [
    "01_data_overview.ipynb",
    "02_data_preprocessing.ipynb",
    "03_eda_and_data_analysis.ipynb",
    "04_data_preparation_for_modeling.ipynb",
    "05_customer_clustering.ipynb"
]

# Tạo thư mục notebooks nếu chưa tồn tại
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_pipeline():
    print(f"==== Customer Personality Analysis Pipeline ====")
    start_time = datetime.now()
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    for notebook in NOTEBOOKS:
        input_path = os.path.join(SOURCE_DIR, notebook)
        output_path = os.path.join(OUTPUT_DIR, notebook.replace(".ipynb", "_executed.ipynb"))

        try:
            print(f"🚀 Running: {input_path}")
            pm.execute_notebook(
                input_path=input_path,
                output_path=output_path,
                parameters={}  # có thể truyền tham số nếu muốn
            )
            print(f"✅ Completed: {output_path}\n")
        except Exception as e:
            print(f"❌ Failed: {notebook}")
            print(f"Error: {e}\n")
            break

    end_time = datetime.now()
    total_duration = end_time - start_time
    print(f"=== Pipeline Finished ===")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Duration: {total_duration}")

if __name__ == "__main__":
    run_pipeline()

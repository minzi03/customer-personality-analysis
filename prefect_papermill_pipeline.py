# pip install prefect papermill

# 1. Start Prefect Orion locally:
# prefect server start

# 2. Run the pipeline using Prefect CLI:
# Visit http://127.0.0.1:4200 to monitor your pipeline in real-time.

from prefect import flow, task
import papermill as pm
import os
from datetime import datetime

# Thư mục notebook gốc
SOURCE_DIR = "src"

# Thư mục lưu notebook sau khi chạy
OUTPUT_DIR = "notebooks"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Danh sách notebook cần chạy
NOTEBOOKS = [
    "01_data_overview.ipynb",
    "02_data_preprocessing.ipynb",
    "03_eda_and_data_analysis.ipynb",
    "04_data_preparation_for_modeling.ipynb",
    "05_customer_clustering.ipynb"
]

@task(retries=2, retry_delay_seconds=10)
def run_notebook(notebook_name: str) -> str:
    """
    Chạy một notebook bằng papermill và lưu output vào thư mục notebooks/.
    """
    input_path = os.path.join(SOURCE_DIR, notebook_name)
    output_path = os.path.join(OUTPUT_DIR, notebook_name.replace(".ipynb", "_executed.ipynb"))

    print(f"🚀 Running: {input_path}")
    pm.execute_notebook(
        input_path=input_path,
        output_path=output_path,
        parameters={}  # Truyền tham số nếu cần
    )
    print(f"✅ Finished: {output_path}")
    return output_path

@flow(name="Customer Personality Analysis Pipeline")
def customer_personality_pipeline():
    """
    Prefect Flow: chạy lần lượt các notebook từ src -> lưu output vào notebooks.
    """
    start_time = datetime.now()
    print(f"🏁 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    output_files = []
    for notebook in NOTEBOOKS:
        output = run_notebook(notebook)
        output_files.append(output)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"🎉 All notebooks executed successfully!")
    print(f"🕒 Total Duration: {duration}")
    return output_files

if __name__ == "__main__":
    customer_personality_pipeline()

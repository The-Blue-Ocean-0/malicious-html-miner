import pandas as pd
import os

# 기본 설정
master_file_path = "urls_data.csv"
batch_folder = "./data/results"  # 배치 파일이 저장된 폴더 경로
output_file_path = "urls_data_with_batches.csv"
excluded_batches = [74, 139, 164, 196, 225, 309, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376,
                    485, 488, 494, 530, 564, 567, 568, 585]
last_batch = 642

# master 데이터 불러오기
master_df = pd.read_csv(master_file_path)

# 배치 병합
for batch_num in range(1, last_batch + 1):
    if batch_num in excluded_batches:
        continue

    batch_filename = f"batch_{batch_num}.csv"
    batch_path = os.path.join(batch_folder, batch_filename)

    if not os.path.exists(batch_path):
        print(f"Missing: {batch_filename}")
        continue

    try:
        batch_df = pd.read_csv(batch_path)
        master_df = pd.merge(master_df, batch_df, on='original_url', how='left', suffixes=('', '_batch'))
        print(f"Merged: {batch_filename} ({len(batch_df)} rows)")
    except Exception as e:
        print(f"Error in {batch_filename}: {e}")

# 저장
master_df.to_csv(output_file_path, index=False)
print(f"전체 병합 결과 저장 완료: {output_file_path}")

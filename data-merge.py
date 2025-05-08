import pandas as pd
import os

# 설정
base_dir = './data/results'  # 배치 파일 경로
input_file = 'urls_data.csv'
output_file = 'urls_data_merge_fixed.csv'
log_file = 'merge_log.txt'

# 누락된 배치 번호
missing_batches = {74, 139, 164, 196, 225, 309, 367, 368, 369, 370, 371, 372,
                   373, 374, 375, 376, 485, 488, 494, 530, 564, 567, 568, 585}
total_batches = 642

# 로그 저장용 리스트
log = []

# 1. 원본 데이터 불러오기
print("Loading urls_data.csv ...")
urls_df = pd.read_csv(input_file, low_memory=False)
log.append(f"Loaded {input_file} with shape {urls_df.shape}")
merged_df = urls_df.copy()

# 2. 배치 병합
for i in range(1, total_batches + 1):
    if i in missing_batches:
        log.append(f"batch_{i}.csv is missing. Skipped.")
        continue

    batch_file = f'batch_{i}.csv'
    batch_path = os.path.join(base_dir, batch_file)

    if not os.path.exists(batch_path):
        log.append(f"{batch_file} not found on disk. Skipped.")
        continue

    try:
        batch_df = pd.read_csv(batch_path)

        # 기존 컬럼 중복 제거
        overlapping_cols = [col for col in batch_df.columns if col in merged_df.columns and col != 'original_url']
        batch_df = batch_df.drop(columns=overlapping_cols)

        # 병합
        before_merge_count = merged_df["original_url"].isin(batch_df["original_url"]).sum()
        merged_df = merged_df.merge(batch_df, on="original_url", how="left")
        log.append(f"{batch_file}: merged with {before_merge_count} matching URLs")

    except Exception as e:
        log.append(f"{batch_file}: error during merge - {e}")

# 3. 결과 저장
merged_df.to_csv(output_file, index=False)
log.append(f"Final merged file saved to {output_file} with shape {merged_df.shape}")
print(f"Done. Merged file saved to: {output_file}")

# 4. 로그 저장
with open(log_file, 'w', encoding='utf-8') as f:
    f.write("\n".join(log))

print(f"Merge log saved to {log_file}")


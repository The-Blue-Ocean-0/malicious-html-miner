import os

# 파라미터 설정
directory_path = r"data\results\batch_results"
expected_range = range(1, 643)  # 1〜642까지 (range는 상한을 포함하지 않기 때문에 643)

# 디렉터리 안의 파일 목록 가져오기
all_files = os.listdir(directory_path)

# batch_번호.csv의 번호만 수집
existing_batches = set()

for file in all_files:
    if file.startswith("batch_") and file.endswith(".csv"):
        try:
            number_str = file[len("batch_"):-len(".csv")]
            number = int(number_str)
            existing_batches.add(number)
        except ValueError:
            # 잘못된 파일 이름(batch_abc.csv 등)은 무시
            continue

# 빠진 번호 찾기
missing_batches = [i for i in expected_range if i not in existing_batches]

# 출력
print("빠진 batch 번호 목록:")
print(missing_batches)

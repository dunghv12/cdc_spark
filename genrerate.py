import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
from cassandra.cluster import Cluster
# Kết nối tới Scylla
cluster = Cluster(['localhost'], port=9042)  # Địa chỉ IP của Scylla container và cổng mặc định 9042
session = cluster.connect('user_data_kp')  # Kết nối tới keyspace user_data


# Số lượng cá nhân cần tạo
num_individuals = 10  # Để tạo hơn 100,000 mối quan hệ, số lượng cá nhân cần khoảng 50,000

# Tạo danh sách UUID cho cá nhân
individuals = [str(uuid.uuid4()) for _ in range(num_individuals)]

# Định nghĩa các loại quan hệ
one_way_relations = ['father', 'mentor', 'teacher']
two_way_relations = ['partner', 'colleague', 'friend']

# Tập hợp để kiểm tra các mối quan hệ một chiều đã tồn tại
one_way_set = set()

# Generate mối quan hệ
relationships = []

for i, from_indiv in enumerate(individuals):
    # Giới hạn số lượng targets được lấy để giảm thời gian xử lý
    num_relations = random.randint(1, 3)  # Mỗi cá nhân có từ 3 đến 6 mối quan hệ
    targets = random.sample(individuals[max(0, i - 5):min(len(individuals), i + 5)], num_relations)
    
    for to_indiv in targets:
        if from_indiv != to_indiv:  # Đảm bảo không tự liên kết chính nó
            # Quyết định loại quan hệ là một chiều hay hai chiều
            if random.random() < 0.5:  # 50% là mối quan hệ một chiều
                relation_type = random.choice(one_way_relations)
                # Tạo khóa duy nhất cho mối quan hệ một chiều
                one_way_key = (from_indiv, to_indiv, relation_type)
                reverse_key = (to_indiv, from_indiv, relation_type)
                if reverse_key not in one_way_set:
                    one_way_set.add(one_way_key)
                    relation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{from_indiv}-{to_indiv}-{relation_type}"))
                    # Sử dụng timestamp hiện tại, phù hợp với Scylla
                    lastupdated = datetime.now().isoformat()
                    relationships.append((from_indiv, to_indiv, relation_type, relation_id, lastupdated))
            else:  # Mối quan hệ hai chiều
                relation_type = random.choice(two_way_relations)
                relation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{from_indiv}-{to_indiv}-{relation_type}")) 
                # Sử dụng timestamp hiện tại, phù hợp với Scylla
                lastupdated = datetime.now().isoformat()
                relationships.append((from_indiv, to_indiv, relation_type, relation_id, lastupdated))
                # Tạo mối quan hệ ngược lại cho hai chiều
                reverse_relation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{to_indiv}-{from_indiv}-{relation_type}"))
                relationships.append((to_indiv, from_indiv, relation_type, reverse_relation_id, lastupdated))

# Chuyển dữ liệu sang DataFrame để dễ quản lý
df_relationships = pd.DataFrame(relationships, columns=['from_indiv', 'to_indiv', 'relation_type', 'relation_id', 'lastupdated'])

# Xuất dữ liệu ra file CSV để có thể dùng NiFi hoặc script khác để chèn vào Scylla
for index, row in df_relationships.iterrows():
    from_indiv = uuid.UUID(row['from_indiv'])
    to_indiv = uuid.UUID(row['to_indiv'])
    relation_type = row['relation_type']
    relation_id = uuid.UUID(row['relation_id'])  # Chuyển từ chuỗi sang UUID
    lastupdated = datetime.fromisoformat(row['lastupdated']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"insert index : {index}")
    session.execute(
        """
        INSERT INTO table_relation (from_indiv, to_indiv, relation_type, relation_id, last_updated)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (from_indiv, to_indiv, relation_type, relation_id, lastupdated)
    )

# Đóng kết nối
cluster.shutdown()
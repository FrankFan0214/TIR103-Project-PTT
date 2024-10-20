from pymongo import MongoClient

# MongoDB 連接字符串
url = "mongodb+srv://TIR103:password6341@tir103.higsi.mongodb.net/?retryWrites=true&w=majority"

# 連接到 MongoDB Atlas，並指定 CA 憑證文件
mongo_client = MongoClient(url, tls=True, tlsCAFile='/etc/ssl/cert.pem')

# 獲取資料庫
db = mongo_client['your_database']

# 測試連接
print("Successfully connected to MongoDB Atlas!")


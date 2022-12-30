from dotenv import load_dotenv, find_dotenv
import os
from pprint import pprint
import json
from pymongo import MongoClient,errors
from bson.objectid import ObjectId

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://Srinivas:{password}@freecluster.yrhda.mongodb.net/test"
client = MongoClient(connection_string)

mydb = client['production']
# print(mydb.list_collection_names())
mycol = mydb['student_data']

with open('students.json') as f:
    file_data = json.load(f)

# client.close()
# 
try:
    # inserts new documents even on error
    mycol.insert_many(file_data,ordered=False, bypass_document_validation=True)
    client.close()

except errors.BulkWriteError as e:
    pass
    # print(f"Articles bulk insertion error {e}")
    panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
    if len(panic_list) > 0:
        pass
        # print(f"these are not duplicate errors {panic_list}")

x = mycol.find_one()
# print(x)


#####################################
'''MAX_RESULT'''
#####################################
def max_result(type):
  max_result=mycol.aggregate([
  { "$unwind": "$scores" },
  { "$group": {
      "_id": "$scores.type",
      "nameScores": {
        "$push": { "name": "$name", "score": "$scores.score" }
      },
      "max": { "$max": "$scores.score" }
    }
  },
  { "$set": {
      "max_score": {
        "$first": {
          "$filter": {
            "input": "$nameScores",
            "cond": { "$eq": [ "$$this.score", "$max" ] }
          }
        }
      }
    }
  },
  { "$project": {
      "_id": 0,
      "type": "$_id",
      "max_score": 1
    }
  }
])
  return max_result

max_marks_exam = [i for i in max_result("exam")]
max_marks_quiz = [i for i in max_result("quiz")]
max_marks_homework = [i for i in max_result("homework")]

# print(max_marks_exam[0])
# print(max_marks_quiz[0])
# print(max_marks_homework[0])

#####################################
'''MIN_RESULT'''
#####################################
def min_result(type):
  min_result=mycol.aggregate([
  { "$unwind": "$scores" },
  { "$group": {
      "_id": "$scores.type",
      "nameScores": {
        "$push": { "name": "$name", "score": "$scores.score" }
      },
      "min": { "$min": "$scores.score" }
    }
  },
  { "$set": {
      "min_score": {
        "$first": {
          "$filter": {
            "input": "$nameScores",
            "cond": { "$eq": [ "$$this.score", "$min" ] }
          }
        }
      }
    }
  },
  { "$project": {
      "_id": 0,
      "type": "$_id",
      "min_score": 40
    }
  }
])
  return min_result

min_marks_exam = [i for i in min_result("exam")]
min_marks_quiz = [i for i in min_result("quiz")]
min_marks_homework = [i for i in min_result("homework")]

# print(min_marks_exam[0])
# print(min_marks_quiz[0])
# print(min_marks_homework[0])


#####################################
'''SUM/AVERAGE'''
#####################################
x = mycol.aggregate(
[

 { '$unwind' : "$scores"},
  { '$group':
     {
       '_id': "$name",
       'total': { '$sum': "$scores.score" }
     }
 }

]
)
for x1 in x:
    print(x1)

mydb1 = client["production"]

mycol1 = mydb1["sum_student_data"]

sum_data = mycol1.insert_many([x1])

mydb2 = client["production"]

mycol2 = mydb2["max_student_data"]

sum_data = mycol2.insert_one(max_marks_exam[0])
sum_data = mycol2.insert_one(max_marks_quiz[0])
sum_data = mycol2.insert_one(max_marks_homework[0])

mydb3 = client["production"]

mycol3 = mydb3["min_student_data"]

sum_data = mycol3.insert_one(min_marks_exam[0])
sum_data = mycol3.insert_one(min_marks_quiz[0])
sum_data = mycol3.insert_one(min_marks_homework[0])
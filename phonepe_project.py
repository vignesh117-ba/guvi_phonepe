import pandas as pd
import os
import json
import psycopg2
import mysql.connector

######CREATING A DATAFRAMES######
#aggregated_insurance
Base_path1=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/aggregated/insurance/country/india/state/"

aggregated_insurance_List= os.listdir(Base_path1)
#print (aggregated_insurance_List)

columns1= {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }

#print(columns1)

for state in aggregated_insurance_List:
    cur_states =Base_path1+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Insurance_type"].append(name)
                columns1["Insurance_count"].append(count)
                columns1["Insurance_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))


aggre_insurance = pd.DataFrame(columns1)
#print(aggre_insurance)

#aggregated_transaction

Base_path2=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/aggregated/transaction/country/india/state/"

aggregated_transaction_List=os.listdir(Base_path2)

#print(aggregated_transaction_List)


columns2 ={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }
print(columns2)

for state in aggregated_transaction_List:
    cur_states =Base_path2+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)

            for i in B["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns2["Transaction_type"].append(name)
                columns2["Transaction_count"].append(count)
                columns2["Transaction_amount"].append(amount)
                columns2["States"].append(state)
                columns2["Years"].append(year)
                columns2["Quarter"].append(int(file.strip(".json")))

aggre_transaction = pd.DataFrame(columns2)

#print(aggre_transaction)

#aggregated_user

Base_path3=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/aggregated/user/country/india/state/"

aggregated_user_List = os.listdir(Base_path3)
#print(aggregated_user_List)

columns3 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}
#print(columns3)

for state in aggregated_user_List:
    cur_states= Base_path3+state+"/"
    agg_year_list=os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            try:

                for i in C["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns3["Brands"].append(brand)
                    columns3["Transaction_count"].append(count)
                    columns3["Percentage"].append(percentage)
                    columns3["States"].append(state)
                    columns3["Years"].append(year)
                    columns3["Quarter"].append(int(file.strip(".json")))
            
            except:
                pass

aggre_user = pd.DataFrame(columns3)
#print(aggre_user)

#map_insurance

base_path4=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/map/insurance/hover/country/india/state/"

map_insurance_list= os.listdir(base_path4)
#print(map_insurance_list)

columns4= {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[],"Transaction_amount":[] }
#print(columns4)

for state in map_insurance_list:
    cur_states =base_path4+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns4["Districts"].append(name)
                columns4["Transaction_count"].append(count)
                columns4["Transaction_amount"].append(amount)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))


map_insurance = pd.DataFrame(columns4)

#print(map_insurance)

#map_transaction
Base_path5=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/map/transaction/hover/country/india/state/"

map_transaction_list = os.listdir(Base_path5)
#print(map_transaction_list)

columns5 = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}
#print(columns5)

for state in map_transaction_list:
    cur_states = Base_path5+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            E = json.load(data)

            for i in E['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns5["District"].append(name)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(columns5)
print(map_transaction)

#map_user
Base_path6=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/map/user/hover/country/india/state/"

map_user_list = os.listdir(Base_path6)
#print(map_user_list)

columns6 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}
#print(columns6)

for state in map_user_list:
    cur_states = Base_path6+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)

            for i in F["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns6["Districts"].append(district)
                columns6["RegisteredUser"].append(registereduser)
                columns6["AppOpens"].append(appopens)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(columns6)
print(map_user)

#top_insurance
Base_path7=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/top/insurance/country/india/state/"

top_insurance_list = os.listdir(Base_path7)
print(top_insurance_list)
columns7 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
print(columns7)

for state in top_insurance_list:
    cur_states = Base_path7+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            G = json.load(data)

            for i in G["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns7["Pincodes"].append(entityName)
                columns7["Transaction_count"].append(count)
                columns7["Transaction_amount"].append(amount)
                columns7["States"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))

top_insur = pd.DataFrame(columns7)
print(top_insur)

#top_transaction
Base_path8=r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/top/transaction/country/india/state/"

top_tran_list = os.listdir(Base_path8)
#print(top_tran_list)

columns8 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
#print(columns8)

for state in top_tran_list:
    cur_states = Base_path8+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns8["Pincodes"].append(entityName)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["States"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(columns8)
#print(top_transaction)

#top_user
Base_path9 =r"C:/Users/vigne/OneDrive/Documents/GitHub/pulse/data/top/user/country/india/state/"

top_user_list = os.listdir(Base_path9)
#print(top_user_list)

columns9 = {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [],'RegisteredUser': []}
#print(columns9)

for state in top_user_list:
    cur_state = Base_path9 + state + "/"
    top_year_list = os.listdir(cur_state)
    
    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)
        
        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            F = json.load(data)
            
            for i in F['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                columns9['Pincodes'].append(name)
                columns9['RegisteredUser'].append(registeredUsers)
                columns9['States'].append(state)
                columns9['Years'].append(year)
                columns9['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(columns9)
#print(df_top_user)

#print("dataframe created successfully")


######DATA INSERTING INTO SQL TABLE ########

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="vigneshk@06!",  
    database="phonepe_data"
)

if mydb.is_connected():
    print("MySQL connection successful!")
    cursor=mydb.cursor()
    
        
#aggre_insur
'''for index, row in aggre_insurance.iterrows():
    insert_query1 = """INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Insurance_type"],
        row["Insurance_count"],
        row["Insurance_amount"]
    )
    try:
        cursor.execute(insert_query1, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in aggregated_insurance: {e} | Values: {values}")

print("Value inserted into aggregated_insurance table successfully")'''

#aggre_trans
'''for index, row in aggre_transaction.iterrows():
    insert_query2 = """INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Transaction_type"],
        row["Transaction_count"],
        row["Transaction_amount"]
    )
    try:
        cursor.execute(insert_query2, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in aggregated_transaction: {e} | Values: {values}")

print("Value inserted into aggregated_transaction table successfully")'''

#aggre_user
'''for index, row in aggre_user.iterrows():
    insert_query3 = """INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Brands"],
        row["Transaction_count"],
        row["Percentage"]
    )
    try:
        cursor.execute(insert_query3, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in aggregated_user: {e} | Values: {values}")

print("Value inserted into aggregated_user table successfully")'''

#map_insur
'''for index, row in map_insurance.iterrows():
    insert_query4 = """INSERT INTO map_insurance (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                       VALUES (%s, %s, %s, %s, %s, %s)"""
    values = (
        row['States'],
        row['Years'],
        row['Quarter'],
        row['Districts'],
        row['Transaction_count'],
        row['Transaction_amount']
    )
    try:
        cursor.execute(insert_query4, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in map_insurance: {e} | Values: {values}")

print("Value inserted into map_insurance table successfully")'''

#map_trans
'''for index, row in map_transaction.iterrows():
    insert_query5 = """INSERT INTO map_transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                       VALUES (%s, %s, %s, %s, %s, %s)"""
    values = (
        row['States'],
        row['Years'],
        row['Quarter'],
        row['District'],
        row['Transaction_count'],
        row['Transaction_amount']
    )
    try:
        cursor.execute(insert_query5, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in map_transaction: {e} | Values: {values}")

print("Value inserted into map_transaction table successfully")'''

#map_user
'''for index, row in map_user.iterrows():
    insert_query6 = """INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Districts"],
        row["RegisteredUser"],
        row["AppOpens"]
    )
    try:
        cursor.execute(insert_query6, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in map_user: {e} | Values: {values}")

print("Value inserted into map_user table successfully")'''

#top_insur
for index, row in top_insur.iterrows():
    insert_query7 = """INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Pincodes"],
        row["Transaction_count"],
        row["Transaction_amount"]
    )
    try:
        cursor.execute(insert_query7, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in top_insurance: {e} | Values: {values}")

#print("Value inserted into top_insurance table successfully")

#top_trans
for index, row in top_transaction.iterrows():
    insert_query8 = """INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Pincodes"],
        row["Transaction_count"],
        row["Transaction_amount"]
    )
    try:
        cursor.execute(insert_query8, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in top_transaction: {e} | Values: {values}")

#print("Value inserted into top_transaction table successfully")

#top_user
for index, row in df_top_user.iterrows():
    insert_query9 = """INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                       values(%s,%s,%s,%s,%s)"""
    values = (
        row["States"],
        row["Years"],
        row["Quarter"],
        row["Pincodes"],
        row["RegisteredUser"]
    )
    try:
        cursor.execute(insert_query9, values)
        mydb.commit()
    except Exception as e:
        print(f"Error in top_user: {e} | Values: {values}")

print("Value inserted into top_user table successfully")

mydb.commit()
cursor.close()
mydb.close()

print("Values inserted successfully.")





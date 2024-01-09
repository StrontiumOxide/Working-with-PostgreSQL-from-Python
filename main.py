from dataclient import ClientBD

input_data = {
    "database": "Client",
    "user": "postgres",
    "password": "NhbybnhjnjkejK02@"
}

call_center = ClientBD("Client", "postgres", "NhbybnhjnjkejK02@")

# call_center.add_client(
#     first_name="Денис",
#     last_name="Дорофеев",
#     email="dorofeevdenis2002@outlook.com"
# )

# call_center.add_phone(
#     phone=79215428101,
#     client_id=1
# )

# call_center.change_client(
#     client_id=1, 
#     first_name="Денис",
#     last_name="Дорофеев",
#     email="dendorof",
#     phones=76666666666
# )

call_center.delete_phone(
    client_id=1,
    phone=76666666666
)

# call_center.__drop_table__()




  
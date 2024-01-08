from dataclient import ClientBD

input_data = {
    "database": "Client",
    "user": "postgres",
    "password": "NhbybnhjnjkejK02@"
}

call_center = ClientBD("Client", "postgres", "NhbybnhjnjkejK02@")

call_center.add_client(
    first_name="Денис",
    last_name="Дорофеев",
    email="dorofeevdenis2002@outlook.com"
)

# call_center.__drop_table__()




  
import psycopg2


class DateError(Exception):
    pass


class ClientBD:

    """
    Данный класс создаётся для создания отдельных экземпляров баз данных внутри Python
    Для инициализации необходимо ввести имя базы данных, пользователя и её пароль.
    """

    def __init__(self, database: str, user: str, password: str) -> None:
        
        try:
                # Проверка соединения с базой данной
            with psycopg2.connect(database=database, user=user, password=password) as conn:
                pass
        except psycopg2.OperationalError:
            raise DateError(f"Невозможно подключиться к базе данных {database}!")
        else:
                # В случае успеха у экземпляра сохраняются входные данные таблицы
                # и автоматически создаются отношения "client" и "client_phone"
            self.database = database
            self.user = user
            self.password = password
            self.__create_table__()

            print(f"Подключение к базе данных {self.database} произведено успешно!")
            

    def __str__(self) -> str:
        return self.database
    

    def __connect_bd__(self, query: str) -> None:

        """
        Данная функция необходима для подключения к базе данных и отправления
        заранее сформированного SQL-запроса.
        """

            # Подключение к базе данных
        with psycopg2.connect(database = self.database, user=self.user, password=self.password) as conn:

                # Создание курсора для работы с базой данных
            with conn.cursor() as cursor:
                cursor.execute(query=query)
                conn.commit()


    def __create_table__(self) -> None:

        """
        Функция для отправки готового SQL-запроса для создания отношений "client" и "client_phone".
        """
    
        query = """
                CREATE TABLE IF NOT EXISTS client (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS client_phone (
                id SERIAl REFERENCES client(id),
                phone_number BIGINT,
                CONSTRAINT id_phone PRIMARY KEY (id, phone_number)
                );
            """
        
        self.__connect_bd__(query=query)


    def __drop_table__(self) -> None:

        """
        Функция для удаления отношений "client" и "client_phone".
        """
    
        query = """
                DROP TABLE client_phone;
                DROP TABLE client;
            """
        
        self.__connect_bd__(query=query)

        print(f"База данных {self.database} полностью очищена!")


    def add_client(self, first_name: str, last_name: str, email: str) -> None:

        """
        Функция для добавления нового пользователя в базу данных.
        Для добавление необходимо передать имя, фамилию и email.
        """

        query = """
                INSERT INTO client (first_name, last_name, email)
                VALUES ('%s', '%s', '%s');
            """ % (first_name, last_name, email)
            
        self.__connect_bd__(query=query)

        print(f"Пользователь {first_name} {last_name} добавлен в базу данных!")


    def add_phone(self, phone: int, client_id: int) -> None:

        """
        Функция для добавления телефона к существующему пользователю в базу данных.
        Для добавление необходимо передать телефон и id_client.
        """

        query = """
                INSERT INTO client_phone (id, phone_number)
                VALUES (%s, %s);
            """ % (client_id, phone)
        
        self.__connect_bd__(query=query)

        print(f"Номер телефона {phone} добавлен пользователю id = {client_id}!")


    def change_client(self, client_id: int, first_name=None, last_name=None, email=None, phones=None) -> None:
        
        """
        Функция для изменения информации о клиенте.
        """

        def created_query(attitude: str, variable: str, variable_1) -> None:

            """
            Функция для создания SQL-запроса взависимости от изменяемых данных.
            """

            query = """
                    UPDATE %s
                    SET %s = '%s'
                    WHERE id = %s
                """ % (attitude, variable, variable_1, client_id)
            
            self.__connect_bd__(query=query)

        if first_name != None:
            created_query(attitude="client", variable="first_name", variable_1=first_name)

        if last_name != None:
            created_query(attitude="client", variable="last_name", variable_1=last_name)

        if email != None:
            created_query(attitude="client", variable="email", variable_1=email)
    
        if phones != None:
            created_query(attitude="client_phone", variable="phone_number", variable_1=phones)

        print("Данные обновлены!")

    
    def delete_phone(self, client_id: int, phone: int) -> None:

        """
        Функция для удаление номером клиента.
        """

        query = """
                DELETE FROM client_phone
                WHERE id = %s and phone_number = %s;
            """ % (client_id, phone)
        
        self.__connect_bd__(query=query)

        print(f"Номер телефона {phone} удалён у пользователя id = {client_id}!")






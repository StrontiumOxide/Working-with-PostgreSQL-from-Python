import psycopg2
import csv
import json


class DateError(Exception):
    def __str__(self) -> str:
        return "Невозможно подключиться к базе данных!"
    

class ConnectBD:

    """
    Данный класс создаётся для отдельных экземпляров баз данных внутри Python
    Для инициализации необходимо ввести имя базы данных, пользователя и её пароль.
    """

    def __init__(self, database: str, user: str, password: str) -> None:
        
        try:
                # Проверка соединения с базой данной
            with psycopg2.connect(database=database, user=user, password=password) as conn:
                pass
        except psycopg2.OperationalError:
            raise DateError
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
    

    def __connect_bd_send_query__(self, query: str) -> None:

        """
        Данная функция необходима для подключения к базе данных и отправления
        заранее сформированного SQL-запроса.\n
        *При SELECT запросах возвращает список кортежей полей
        """

            # Подключение к базе данных
        with psycopg2.connect(database = self.database, user=self.user, password=self.password) as conn:

                # Создание курсора для работы с базой данных
            with conn.cursor() as cursor:
                cursor.execute(query=query)
                conn.commit()

                try:
                    data = cursor.fetchall()
                except psycopg2.ProgrammingError: 
                    pass
                else:
                    return data


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
                phone_number BIGINT UNIQUE,
                CONSTRAINT id_phone PRIMARY KEY (id, phone_number)
                );
            """
        
        self.__connect_bd_send_query__(query=query)


    def __drop_table__(self) -> None:

        """
        Функция для удаления отношений "client" и "client_phone".
        """
    
        query = """
                DROP TABLE client_phone;
                DROP TABLE client;
            """
        
        self.__connect_bd_send_query__(query=query)

        print(f"База данных {self.database} полностью очищена!")


    def add_client(self, first_name: str, last_name: str, email: str) -> None:

        """
        Функция для добавления нового пользователя в базу данных.\n
        Для добавление необходимо передать имя, фамилию и email.
        """

        query = """
                INSERT INTO client (first_name, last_name, email)
                VALUES ('%s', '%s', '%s');
            """ % (first_name, last_name, email)
            
        self.__connect_bd_send_query__(query=query)

        print(f"Пользователь {first_name} {last_name} добавлен в базу данных!")


    def add_phone(self, phone: int, client_id: int) -> None:

        """
        Функция для добавления телефона к существующему пользователю в базу данных.\n
        Для добавление необходимо передать телефон и id_client.
        """

        query = """
                INSERT INTO client_phone (id, phone_number)
                VALUES (%s, %s);
            """ % (client_id, phone)
        
        self.__connect_bd_send_query__(query=query)

        print(f"Номер телефона {phone} добавлен пользователю id = {client_id}!")


    def change_client(self, client_id: int, first_name=None, 
                      last_name=None, email=None, phone_old=None, 
                      phone_new=None) -> None:
        
        """
        Функция для изменения информации о клиенте.
        """

        def created_query(variable: str, variable_1: str) -> None:

            """
            Функция для создания SQL-запроса взависимости от изменяемых данных.
            """

            query = """
                    UPDATE client
                    SET %s = '%s'
                    WHERE id = %s;
                """ % (variable, variable_1, client_id)
            
            self.__connect_bd_send_query__(query=query)

        if first_name != None:
            created_query(variable="first_name", variable_1=first_name)

        if last_name != None:
            created_query(variable="last_name", variable_1=last_name)

        if email != None:
            created_query(variable="email", variable_1=email)
    
        if phone_old != None and phone_new != None:

            query = """
                    UPDATE client_phone
                    SET phone_number = '%s'
                    WHERE id = %s and phone_number = %s;
                """ % (phone_new, client_id, phone_old)
            
            self.__connect_bd_send_query__(query=query)

        print("Данные обновлены!")

    
    def delete_phone(self, client_id: int, phone: int) -> None:

        """
        Функция для удаление номеров клиента.
        """

        query = """
                DELETE FROM client_phone
                WHERE id = %s and phone_number = %s;
            """ % (client_id, phone)
        
        self.__connect_bd_send_query__(query=query)

        print(f"Номер телефона {phone} удалён у пользователя id = {client_id}!")


    def delete_client(self, client_id: int) -> None:

        """
        Функция для удаление клиента.
        """

        query_select = """
                    SELECT * FROM client_phone
                    WHERE id = %s
                """ % client_id
        
        for _id_, _ in self.__connect_bd_send_query__(query_select):

            query_delete = """
                    DELETE FROM client_phone
                    WHERE id = %s
                """ % _id_
            
            self.__connect_bd_send_query__(query_delete)

        query = """
                DELETE FROM client
                WHERE id = %s;
            """ % client_id
        
        self.__connect_bd_send_query__(query=query)

        print(f"Удалён пользователь id = {client_id}!")


    def find_client(self, first_name=None, last_name=None, email=None, phone=None) -> list:

        """
        Функция для поиска клиента по его данным.\n
        Возвращает список кортежей полей
        """

        def created_query(variable: str, variable_1: str) -> None:

            """
            Функция для создания SQL-запроса взависимости от изменяемых данных.
            """

            query = """
                    SELECT c.id, c.first_name, c.last_name, c.email, cp.phone_number FROM client c
                    JOIN client_phone cp ON cp.id = c.id 
                    WHERE %s = '%s';
                """ % (variable, variable_1)
            
            
            return self.__connect_bd_send_query__(query=query)
        
            # Создание списка data_check, в котором будут храниться критерии по отбору информации
            # Формирование списка по каждому отдельному критерию
        data_check = []

        first_name_data = []
        if first_name != None:
            first_name_data = created_query(variable="first_name", variable_1=first_name)
            data_check.append(first_name)
        
        last_name_data = []
        if last_name != None:
            last_name_data = created_query(variable="last_name", variable_1=last_name)
            data_check.append(last_name)

        email_data = []
        if email != None:
            email_data = created_query(variable="email", variable_1=email)
            data_check.append(email)
    
        phone_datadata = []
        if phone != None:
            phone_datadata = created_query(variable="phone_number", variable_1=phone)
            data_check.append(phone)

            # Объединение всех списков. Избавление от дубликатов
        dirty_data = []
        for info_list in [first_name_data, last_name_data, email_data, phone_datadata]:
            for element in info_list:
                dirty_data.append(element)
        dirty_data = list(set(dirty_data))

            # Формирование общего списка. В каждом подсписке будет содержаться информация
            # селективно отобранная по одному из критериев в data_check
        level_selection = 0
        max_level_selection = len(data_check)
        clean_data = [[] for i in range(max_level_selection)]
        clean_data.insert(0, dirty_data)

        for _ in range(max_level_selection):

            for element in clean_data[level_selection]:

                if data_check[level_selection] in element:
                    clean_data[level_selection+1].append(element)

            level_selection += 1

        return clean_data[-1]
            
        
    def load_full_info(self, create_csv=False, create_json=False) -> None:

        """
        Функция для выгрузки всей информации из базы данных.\n
        Выберите в каком виде вы хотите выгрузить информацию.\n
        Выгрузить информацию можно в CSV или JSON форматах
        """
        
        query = """
                SELECT  c.id AS "id-пользователя", 
                c.first_name AS "Имя", 
                c.last_name AS "Фамилия", 
                c.email AS "Почта", 
                cp.phone_number AS "Номер телефона"
                FROM client_phone cp 
                FULL jOIN client c ON c.id = cp.id  
                ORDER BY c.id 
            """
        
        data = self.__connect_bd_send_query__(query=query)

        if create_csv:

                # Создание CSV-файла
            with open(file=f"{self.database}.csv", mode="w", encoding="utf-8-sig", newline="") as file:
            
                field = ["id-пользователя", "Имя", "Фамилия", "Почта", "Номер телефона"]
                writer = csv.writer(file, delimiter=";")
                writer.writerow(field)
                writer.writerows(data)

        if create_json:

                # Создание JSON-файла
            with open(file=f"{self.database}.json", mode="w", encoding="utf-8-sig", newline="") as file:

                json.dump(data, file, ensure_ascii=False, indent=2)











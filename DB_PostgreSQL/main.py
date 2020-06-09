import psycopg2 as pg


class HwDB:
    def __init__(self, database, user, password):
        self.params = {
            'database': database,
            'user': user,
            'password': password,
            'host': 'localhost',
            'port': 5432,
        }

        self.create_db()


    def create_db(self):
        with pg.connect(**self.params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS student(
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        gpa NUMERIC(10, 2),
                        birth TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS course(
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS student_course(
                        student_id INT REFERENCES student(id),
                        course_id INT REFERENCES course(id),
                        PRIMARY KEY (student_id, course_id)
                    );
                """)


    def add_course(self, course_name):
        with pg.connect(**self.params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO course (name)
                    VALUES (%s)
                    RETURNING id;
                """, (course_name,))
                course_id = cursor.fetchone()[0]
                print(f'Курс c id={course_id} создан')
                return course_id

    def add_student(self, student):
        with pg.connect(**self.params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO student (name, gpa, birth) VALUES (%s, %s, %s)
                    RETURNING id;
                """, (student.get('name'), student.get('gpa', 0),
                      student.get('birth', '1990-01-01')))
                student_id = cursor.fetchone()[0]
                return student_id

    def get_student(self, student_id):
        with pg.connect(**self.params) as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM Student WHERE id = %s;', (student_id,))
                return cursor.fetchone()

    def add_students(self, course_name, students):
        with pg.connect(**self.params) as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT id FROM course WHERE course.name = %s', (course_name,))
                response = cursor.fetchone()
                if not response:
                    print('Такого курса нет')
                    return
                course_id = response[0]

                for student in students:
                    student_id = self.add_student(student)
                    cursor.execute(
                        'INSERT INTO student_course (student_id, course_id) VALUES (%s, %s);',
                        (student_id, course_id)
                    )

    def get_students(self, course_name):
        with pg.connect(**self.params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT s.name
                    FROM student_course AS s_c
                    JOIN student AS s ON s_c.student_id = s.id
                    JOIN course AS c on s_c.course_id = c.id
                    WHERE c.name = %s;
                """, (course_name,))
                return [response[0] for response in cursor.fetchall()]


if __name__ == '__main__':
    my_db = HwDB('hw_test_db', 'postgres', 'postgres')

    my_db.add_course('Python-разработчик')
    my_db.add_course('Java-разработчик')

    students = [
        {
            'name': 'Дмитрий',
            'gpa': '2.8',
            'birth': '1991-12-25',
        },
        {
            'name': 'Иван',
            'gpa': '9.0',
            'birth': '1997-09-12',
        },
    ]
    my_db.add_students('Python-разработчик', students)

    print(my_db.get_students('Python-разработчик'))
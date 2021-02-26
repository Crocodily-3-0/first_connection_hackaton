import pymysql
import pymysql.cursors

from config import MYSQL_DB, MYSQL_USER, MYSQL_HOST, MYSQL_PWD, MYSQL_PORT


user_content = {}


class Db:
    def connect(self):
        """
        Подключение к базе данных
        """
        self.conn = pymysql.connect(host=MYSQL_HOST,
                                    user=MYSQL_USER,
                                    password=MYSQL_PWD,
                                    port=MYSQL_PORT,
                                    db=MYSQL_DB,
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    def query(self, query, params=()):
        """
        Запрос к базе данных
        """
        try:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
        except pymysql.OperationalError:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
        finally:
            self.conn.close()
        return cursor

    def get_new_stats(self, user_id):
        sql = "SELECT `last_page`, `content_id` FROM `new_stats` WHERE `reader_id`=%s"
        self.request = self.query(sql, user_id)
        self.result = self.request.fetchall()
        print("Db().get_new_stats(): Done")
        stats = self.result
        for stat in stats:
            last = int(stat['last_page'])
            content = stat['content_id']
            sql = "SELECT `real_bnumber` FROM `Contents` WHERE `content_id`=%s"
            self.request = self.query(sql, content)
            self.result = self.request.fetchone()
            print("Db().get_Contents(): Done")
            pages = int(self.result['real_bnumber'])
            if last == pages:
                exist = self.count_from_content(content, user_id)
                if not exist:
                    user_content[user_id] = [content]
                    # sql = "INSERT INTO `content_table` (`user_id`, `content`) VALUES (%s, %s)"
                    # self.request = self.query(sql, (user_id, content))
                    # print("Db().add_content_table(): Added")
                else:
                    user_content[user_id].append(content)

    def get_new_stats_course(self, user_id):
        sql = "SELECT `stat_id` FROM `new_stats` WHERE `reader_id`=%s"
        self.request = self.query(sql, user_id)
        self.result = self.request.fetchall()
        print("Db().get_new_stats(): Done")
        stats = self.result
        for stat in stats:
            stat_id = stat["stat_id"]
            sql = "SELECT `course_id` FROM `new_stats_course` WHERE `stat_id`=%s"
            self.request = self.query(sql, stat_id)
            self.result = self.request.fetchone()
            print("Db().get_new_stats_course(): Done")
            course_id = self.result["course_id"]
            exist = self.count_from_content(course_id, user_id)
            if not exist:
                user_content[user_id] = [course_id]
                # sql = "INSERT INTO `content_table` (`user_id`, `content`) VALUES (%s, %s)"
                # self.request = self.query(sql, (user_id, course_id))
                # print("Db().add_content_table(): Added")
            else:
                user_content[user_id].append(course_id)

    def create_rating_table(self):
        query = "CREATE TABLE rating (" \
                "id INT NOT NULL AUTO_INCREMENT," \
                "user_id CHAR(36) NOT NULL," \
                "rating INT NOT NULL," \
                "PRIMARY KEY (id)," \
                "FOREIGN KEY (user_id)  REFERENCES ebs_users (ebs_user_id)" \
                ");"
        self.request = self.query(query)
        return 0

    def create_content_table(self):
        query = "CREATE TABLE content_table (" \
                "id INT NOT NULL AUTO_INCREMENT," \
                "user_id CHAR(36) NOT NULL," \
                "content CHAR(36) NOT NULL," \
                "PRIMARY KEY (id)," \
                "FOREIGN KEY (user_id)  REFERENCES ebs_users (ebs_user_id)," \
                "FOREIGN KEY (content)  REFERENCES Contents (content_id)" \
                ");"
        self.request = self.query(query)
        return 0

    def add_user_rating(self, user_id, rating):
        exist = self.count_from_rating(user_id)
        if not exist:
            sql = "INSERT INTO `rating` (`user_id`, `rating`) VALUES (%s, %s)"
            self.request = self.query(sql, (user_id, rating))
            print("Db().add_user_rating(): Added")
        else:
            sql = "UPDATE `rating` SET `rating`=%s WHERE `user_id`=%s"
            self.request = self.query(sql, (rating, user_id))
            print("Db().add_user_rating(): Updated")

    def get_users(self):
        sql = "SELECT * FROM `ebs_users`"
        self.request = self.query(sql)
        self.result = self.request.fetchall()
        print("Db().get_users(): Done")

        return self.result

    def count_from_rating(self, user_id):
        sql = "SELECT COUNT(*) FROM `rating` WHERE `user_id`=%s"
        self.request = self.query(sql, user_id)
        self.result = self.request.fetchone()
        print("Db().count_from_rating(): Counted")
        if self.result['COUNT(*)'] > 0:
            return True
        return False

    def count_from_content_by_user(self, user_id):
        if user_id in user_content.keys():
            return len(user_content[user_id])
        return 0
        # sql = "SELECT COUNT(*) FROM `content_table` WHERE `user_id`=%s"
        # self.request = self.query(sql, user_id)
        # self.result = self.request.fetchone()
        # print("Db().count_from_content_by_user(): Counted")
        # return int(self.result['COUNT(*)'])

    def count_from_content(self, content_id, user_id):
        if user_id in user_content.keys() and content_id in user_content[user_id]:
            return True
        return False
        # sql = "SELECT COUNT(*) FROM `content_table` WHERE `content`=%s AND `user_id`=%s"
        # self.request = self.query(sql, (content_id, user_id))
        # self.result = self.request.fetchone()
        # print("Db().count_from_content(): Counted")
        # if self.result['COUNT(*)'] > 0:
        #     return True
        # return False

    def get_user_from_rating(self, user_id):
        exist = self.count_from_rating(user_id)
        if exist:
            sql = "SELECT * FROM `rating` WHERE `user_id`=%s"
            self.request = self.query(sql, user_id)
            self.result = self.request.fetchone()
            print("Db().get_user(): Got")
            return int(self.result["rating"])
        else:
            return 0

    def get_quiz(self, user_id):
        sql = "SELECT `score`, `total_score` FROM `quiz_users` WHERE `ebs_user_id`=%s"
        self.request = self.query(sql, user_id)
        self.result = self.request.fetchall()
        return self.result

    def get_final_quiz(self, user_id):
        sql = "SELECT `score`, `total_score`, `data_id`, `exam_id` FROM `final_quiz_users` WHERE `ebs_user_id`=%s"
        self.request = self.query(sql, user_id)
        self.result = self.request.fetchall()
        scores = 0
        total_scores = 0
        for quizzes in self.result:
            content = quizzes["data_id"]
            exist = self.count_from_content(content, user_id)
            if exist:
                scores += quizzes["score"]
                exam_id = quizzes["exam_id"]
                exam = self.get_exam(exam_id, user_id)
                if exam:
                    total_scores += quizzes["total_score"]

        return {"scores": scores, "total_scores": total_scores}

    def get_exam(self, exam_id, user_id):
        sql = "SELECT `group_id` FROM `quiz_exams` WHERE `exam_id`=%s"
        self.request = self.query(sql, exam_id)
        self.result = self.request.fetchone()
        group_id = self.result["group_id"]
        sql = "SELECT `status` FROM `student_groups` WHERE (`ebs_user_id`=%s AND `student_group_id`=%s)"
        self.request = self.query(sql, (user_id, group_id))
        self.result = self.request.fetchone()
        return int(self.result["status"])


def test():
    """
    Тестовая функция
    """
    db = Db()
    result = db.show_colums()
    for res in result:
        print(res)


# test()
tables = [
    "ebs_users",
    "rating",
    "quiz_users",
    "final_quiz_users",
    "Contents",
    "student_groups",
    "`ebs_users`"
]


def show_colums(self):
    """
    Количество строк в таблице с данным именем
    """
    query = "SHOW columns FROM `new_stats_course`"
    self.request = self.query(query)
    self.result = self.request.fetchall()
    print("Db().show_colums(): Done")
    return self.result
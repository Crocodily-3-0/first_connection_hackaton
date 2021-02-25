from src.db.db import Db


def get_rating(user_id):
    db = Db()
    quiz = db.get_quiz(user_id)
    score_quiz = quiz["score"]
    max_score_quiz = quiz["total_score"]
    final_quiz = db.get_final_quiz(user_id)
    score_final_quiz = final_quiz["score"]
    max_score_final_quiz = 0
    if final_quiz["exam"]:
        max_score_final_quiz = final_quiz["total_score"]

    return score_quiz + score_final_quiz + max_score_final_quiz + max_score_quiz


def init():
    """
    Здесь мы создаем таблицу с user_id и rating
    Проходим по всем пользователям, делаем проверку доступа, вычисляем рейтинг, добавляем в таблицу
    """
    db = Db()
    try:
        db.create_rating_table()
        db.create_content_table()
    except:
        print("db probably exist")
    users = db.get_users()
    for user_id in users:
        db.get_new_stats(user_id)
    for user_id in users:
        rating = get_rating(user_id)
        db.add_user_rating(user_id, rating)
    return 0


def main(user_id):
    """
    Делаем запрос в таблицу rating. Если данного пользователя нет, значит он не выполнил условия.
    """
    db = Db()
    user_rating = db.get_user_from_rating(user_id)
    if user_rating:
        print(user_rating)
    else:
        print("Пользователь не участвует в рейтинге")
    return 0


if __name__ == '__main__':
    user = 1
    init()
    main(user)

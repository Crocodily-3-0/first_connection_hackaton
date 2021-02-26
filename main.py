from src.db.db import Db


rating_list = []
user_rating = {}


def get_rating(user_id):
    db = Db()
    quizzes = db.get_quiz(user_id)  # get all quizes for this user
    scores_quiz = 0  # scores for all quizes this user
    for quiz in quizzes:
        scores_quiz += quiz["score"]

    final_quiz = db.get_final_quiz(user_id)
    score_final_quiz = final_quiz["scores"]
    max_score_final_quiz = final_quiz["total_scores"]

    pass_contents = db.count_from_content_by_user(user_id)

    return scores_quiz + score_final_quiz + max_score_final_quiz + pass_contents


def init():
    """
    Здесь мы создаем таблицу с user_id и rating
    Проходим по всем пользователям, делаем проверку доступа, вычисляем рейтинг, добавляем в таблицу
    """
    db = Db()
    # db.create_rating_table()
    # db.create_content_table()
    users = db.get_users()
    for user in users:
        user_id = user["ebs_user_id"]
        db.get_new_stats(user_id)
        # db.get_new_stats_course(user_id)
        rating = get_rating(user_id)
        # db.add_user_rating(user_id, rating)
        rating_list.append(rating)
        user_rating[user_id] = rating
    return 0


def main(user_id):
    """
    Делаем запрос в таблицу rating. Если данного пользователя нет, значит он не выполнил условия.
    """
    # db = Db()
    # user_rating = db.get_user_from_rating(user_id)
    rating = user_rating[user_id]
    if rating:
        print(f"Количество баллов студента в рейтинге: {rating}")
        rating_list.sort()
        print(f"Рейтинг студента: {rating_list.index(rating)}")
    else:
        print("Пользователь не участвует в рейтинге")
    return 0


if __name__ == '__main__':
    id = "70B6BB5B-3262-4AE0-A8DE-4633FBD804D2"
    init()
    print(f"Рейтинг пользователя с id={id}:")
    main(user_id=id)

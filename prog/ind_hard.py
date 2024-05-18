#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import typing as t
from pathlib import Path
import psycopg2


def connect():
    conn = psycopg2.connect(
        user="postgres",
        password="Password",
        host="127.0.0.1",
        port="5432",
        database="postgres"
    )

    return conn


def display_students(staff: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Вывести список студентов
    """
    # Заголовок таблицы.
    if staff:

        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 15
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^15} |'.format(
                "№",
                "Ф.И.О.",
                "Группа",
                "Успеваемость"
            )
        )
        print(line)

        # Вывести данные о всех студентах.
        for idx, student in enumerate(staff, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>15} |'.format(
                    idx,
                    student.get('name', ''),
                    student.get('group', ''),
                    student.get('grade', 0)
                )
            )
        print(line)
    else:
        print("Список студентов пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Создать таблицу с информацией о группах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о студентах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_name TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            student_grade LIST NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(group_id)
        )
        """
    )

    conn.close()


def add_student(
        database_path: Path,
        name: str,
        group: str,
        grade: list):
    """
    Добавить данные о студенте
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Получить идентификатор группы в базе данных.
    # Если такой записи нет, то добавить информацию о новой группе.
    cursor.execute(
        """
        SELECT group_id FROM groups WHERE group_title = ?
        """,
        (group,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO groups (group_title) VALUES (?)
            """,
            (group,)
        )
        group_id = cursor.lastrowid

    else:
        group_id = row[0]

    # Добавить информацию о новом самолете.
    cursor.execute(
        """
        INSERT INTO students (student_name, group_id, student_grade)
        VALUES (?, ?, ?)
        """,
        (name, group_id, grade)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех студентов.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT students.student_name, groups.group_title, students.student_grade
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
        "name": row[0],
        "group": row[1],
        "grade": row[2],
        }
        for row in rows
    ]


def select_by_grade(
    database_path: Path, group: int
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать студентов с заданной успеваемостью.
    """

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT students.student_name, groups.group_title, students.student_grade
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        WHERE groups.group_title = ?
        """,
        (group,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "name": row[0],
            "group": row[1],
            "grade": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.cwd() / "ind_hard.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("students")
    parser.add_argument(
        "--version",
        action="version",
        help="The main parser",
        version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления студента.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new student"
    )
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The student's name"
    )
    add.add_argument(
        "-g",
        "--group",
        type=int,
        action="store",
        help="The student's group"
    )
    add.add_argument(
        "-gr",
        "--grade",
        action="store",
        required=True,
        help="The student's grade"
    )

    # Создать субпарсер для отображения всех студентов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all students"
    )

    # Создать субпарсер для выбора студентов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the students"
    )
    select.add_argument(
        "-s",
        "--select",
        action="store",
        required=True,
        help="The required select"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить студента.

    if args.command == "add":
        add_student(db_path, args.name, args.group, args.grade)

    # Отобразить всех студентов.
    elif args.command == "display":
        display_students(select_all(db_path))

    # Выбрать требуемых студентов.
    elif args.command == "select":
        display_students(select_by_grade(db_path, args.select))
        pass


if __name__ == '__main__':
    main()
# views.py
from typing import Any, Dict, Iterable, Optional


def prompt(msg: str) -> str:
    return input(f"{msg}: ").strip()


def prompt_nullable(msg: str, default: Optional[str] = None) -> Optional[str]:
    s = input(f"{msg} [{default if default is not None else ''}]: ").strip()
    if s == "":
        return None
    return s


def show_message(msg: str):
    print(msg)


def show_error(msg: str):
    print("Помилка:", msg)


def show_success(msg: str):
    print("Успіх:", msg)


def print_rows(rows: Iterable[Dict[str, Any]], max_rows: int = 200):
    rows = list(rows)
    if not rows:
        print("Немає даних.")
        return
    rows = rows[:max_rows]
    headers = list(rows[0].keys())
    widths = [len(h) for h in headers]
    for row in rows:
        for i, h in enumerate(headers):
            widths[i] = max(widths[i], len(str(row[h])))

    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))

    for row in rows:
        line = " | ".join(str(row[h]).ljust(widths[i]) for i, h in enumerate(headers))
        print(line)

    if len(rows) == max_rows:
        print(f"... Показано перші {max_rows} рядків.")


def print_row(row: Optional[Dict[str, Any]]):
    if not row:
        print("Запис не знайдено.")
        return
    for k, v in row.items():
        print(f"{k}: {v}")


def show_query_result(rows, exec_time_ms):
    print_rows(rows, max_rows=200)
    if exec_time_ms is not None:
        print(f"\nЧас виконання запиту: {exec_time_ms:.2f} ms")
    else:
        print("\nЧас виконання: недоступний")


def main_menu():
    print("\n=== Платформа доставки їжі  ===")
    print("1 - Показати всі записи таблиці")
    print("2 - Показати запис за PK")
    print("3 - Пошук (3 складні запити)")
    print("4 - Вставка запису")
    print("5 - Оновлення запису")
    print("6 - Видалення запису")
    print("7 - Генерація тестових даних")
    print("0 - Вихід")


def choose_table_menu(tables):
    print("\nДоступні таблиці:")
    for i, t in enumerate(tables, start=1):
        print(f"{i}. {t}")
    s = input("Оберіть таблицю за номером: ").strip()
    try:
        idx = int(s)
        if 1 <= idx <= len(tables):
            return tables[idx - 1]
    except ValueError:
        pass
    return None
# main.py
from controllers import Controller
import psycopg
import views
from config import DB

def main():
    # Перевірка підключення
    try:
        conn = psycopg.connect(**DB)
        conn.close()
    except Exception as e:
        views.show_error(f"Не вдалося підключитися до БД: {e}")
        return

    ctrl = Controller()
    try:
        ctrl.run()
    finally:
        ctrl.close()

if __name__ == "__main__":
    main()
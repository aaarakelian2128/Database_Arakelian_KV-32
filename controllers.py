# controllers.py
from typing import Any, Dict
import psycopg
import views
from model import Model


class Controller:
    def __init__(self):
        self.model = Model()
        self.tables = self.model.list_tables()

    def close(self):
        self.model.close()

    # -------- допоміжне --------
    def _choose_table(self) -> str:
        table = views.choose_table_menu(self.tables)
        if not table:
            views.show_error("Невірний вибір таблиці.")
        return table

    def _read_pk_value(self, table: str, pk: str):
        cols = self.model.columns_info(table)
        pk_col = next((c for c in cols if c["name"] == pk), None)
        if not pk_col:
            return None
        raw = views.prompt(f"Значення PK ({pk})")
        try:
            val = self.model.cast_value(raw, pk_col["type"])
            return val
        except Exception as e:
            views.show_error(str(e))
            return None

    # -------- дії --------
    def action_show_all(self):
        table = self._choose_table()
        if not table:
            return
        try:
            rows = self.model.select_all(table)
            views.print_rows(rows)
        except psycopg.Error as e:
            views.show_error(f"Помилка БД: {e}")

    def action_show_by_pk(self):
        table = self._choose_table()
        if not table:
            return
        pk = self.model.primary_key(table)
        if not pk:
            views.show_error("Не знайдено PK для таблиці.")
            return
        pk_val = self._read_pk_value(table, pk)
        if pk_val is None:
            return
        try:
            row = self.model.select_by_pk(table, pk, pk_val)
            views.print_row(row)
        except psycopg.Error as e:
            views.show_error(f"Помилка БД: {e}")

    def action_insert(self):
        table = self._choose_table()
        if not table:
            return
        cols_info = self.model.columns_info(table)
        data: Dict[str, Any] = {}
        for c in cols_info:
            if c["identity"]:
                # серіальний PK не питаємо
                continue
            name = c["name"]
            dtype = c["type"]
            raw = views.prompt_nullable(f'{name} ({dtype})')
            if raw is None:
                if not c["nullable"]:
                    views.show_error(f"Поле {name} не може бути NULL.")
                    return
                value = None
            else:
                try:
                    value = self.model.cast_value(raw, dtype)
                except Exception as e:
                    views.show_error(str(e))
                    return
            data[name] = value

        ok, err = self.model.insert(table, data)
        if ok:
            views.show_success("Запис додано успішно.")
        else:
            views.show_error(f"Не вдалося додати запис: {err}")

    def action_update(self):
        table = self._choose_table()
        if not table:
            return
        pk = self.model.primary_key(table)
        if not pk:
            views.show_error("Не знайдено PK для таблиці.")
            return
        pk_val = self._read_pk_value(table, pk)
        if pk_val is None:
            return

        row = self.model.select_by_pk(table, pk, pk_val)
        if not row:
            views.show_error("Рядок не знайдено.")
            return

        views.show_message("Введіть нові значення (Enter — залишити поточне).")
        cols_info = self.model.columns_info(table)
        updates: Dict[str, Any] = {}

        for c in cols_info:
            name = c["name"]
            if name == pk:
                continue
            cur_val = row.get(name)
            raw = views.prompt_nullable(f"{name} (поточне: {cur_val})")
            if raw is None:
                continue
            try:
                new_val = self.model.cast_value(raw, c["type"])
            except Exception as e:
                views.show_error(str(e))
                return
            updates[name] = new_val

        if not updates:
            views.show_message("Нічого не змінено.")
            return

        # Валідація зовнішніх ключів перед оновленням
        if table == "ordering":
            for parent_col, parent_table in [
                ("client_id", "client"),
                ("courier_id", "courier"),
                ("order_id", "Order"),
            ]:
                if parent_col in updates:
                    parent_pk = self.model.primary_key(parent_table)
                    val = updates[parent_col]
                    parent_row = self.model.select_by_pk(parent_table, parent_pk, val)
                    if not parent_row:
                        views.show_error(f"{parent_col}: запис у {parent_table} не знайдено.")
                        return

        ok, err = self.model.update(table, pk, pk_val, updates)
        if ok:
            views.show_success("Запис оновлено успішно.")
        else:
            views.show_error(f"Не вдалося оновити запис: {err}")

    def action_delete(self):
        table = self._choose_table()
        if not table:
            return
        pk = self.model.primary_key(table)
        if not pk:
            views.show_error("Не знайдено PK для таблиці.")
            return
        pk_val = self._read_pk_value(table, pk)
        if pk_val is None:
            return

        ok, err = self.model.delete(table, pk, pk_val)
        if ok:
            views.show_success("Рядок успішно видалено.")
        else:
            views.show_error(err or "Помилка при видаленні.")

    def action_generate(self):
        views.show_message("Генерація тестових даних (SQL random()).")
        try:
            n_clients = int(views.prompt("Скільки клієнтів (0 - пропустити)"))
            n_couriers = int(views.prompt("Скільки кур'єрів (0 - пропустити)"))
            n_orders = int(views.prompt("Скільки замовлень (Order) (0 - пропустити)"))
            n_ordering = int(views.prompt("Скільки зв'язків (ordering) (0 - пропустити)"))
            n_dishes = int(views.prompt("Скільки страв (Dish) (0 - пропустити)"))
        except ValueError:
            views.show_error("Потрібно вводити цілі числа.")
            return

        try:
            if n_clients > 0:
                self.model.generate_clients(n_clients)
            if n_couriers > 0:
                self.model.generate_couriers(n_couriers)
            if n_orders > 0:
                self.model.generate_orders(n_orders)
            if n_ordering > 0:
                self.model.generate_ordering(n_ordering)
            if n_dishes > 0:
                self.model.generate_dishes(n_dishes)

            views.show_success("Генерацію даних завершено.")
        except psycopg.Error as e:
            views.show_error(f"Помилка БД при генерації: {e}")

    def action_search(self):
        while True:
            print("\n--- Пошук (Delivery) ---")
            print("1 - Статистика клієнтів (замовлення + витрати, фільтр по імені)")
            print("2 - Продуктивність кур'єрів (фільтр по транспорту)")
            print("3 - Пошук страв за діапазоном цін")
            print("0 - Назад")
            choice = input("Виберіть опцію: ").strip()

            if choice == "1":
                name = views.prompt("Фрагмент імені клієнта")
                try:
                    rows, t_ms = self.model.search_clients_orders_stats(name)
                    views.show_query_result(rows, t_ms)
                except psycopg.Error as e:
                    views.show_error(f"Помилка БД: {e}")
            elif choice == "2":
                transport = views.prompt("Тип транспорту (Car, Bicycle, Foot...)")
                try:
                    rows, t_ms = self.model.search_couriers_transport_stats(transport)
                    views.show_query_result(rows, t_ms)
                except psycopg.Error as e:
                    views.show_error(f"Помилка БД: {e}")
            elif choice == "3":
                try:
                    min_p = int(views.prompt("Мін. ціна"))
                    max_p = int(views.prompt("Макс. ціна"))
                    rows, t_ms = self.model.search_dishes_price_range(min_p, max_p)
                    views.show_query_result(rows, t_ms)
                except ValueError:
                    views.show_error("Треба ввести числа.")
                except psycopg.Error as e:
                    views.show_error(f"Помилка БД: {e}")
            elif choice == "0":
                break
            else:
                views.show_error("Невірний вибір.")

    def run(self):
        while True:
            views.main_menu()
            choice = input("Виберіть опцію: ").strip()
            if choice == "1":
                self.action_show_all()
            elif choice == "2":
                self.action_show_by_pk()
            elif choice == "3":
                self.action_search()
            elif choice == "4":
                self.action_insert()
            elif choice == "5":
                self.action_update()
            elif choice == "6":
                self.action_delete()
            elif choice == "7":
                self.action_generate()
            elif choice == "0":
                break
            else:
                views.show_error("Невірний вибір, спробуйте ще раз.")
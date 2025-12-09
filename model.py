# model.py
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import psycopg
from psycopg.rows import dict_row
from config import DB

class Model:

    def __init__(self):
        self.conn = psycopg.connect(**DB, row_factory=dict_row)

        self.tables = {
            "client": {
                "pk": "client_id",
                "children": [("ordering", "client_id")]
            },
            "courier": {
                "pk": "courier_id",
                "children": [("ordering", "courier_id")]
            },
            "Order": {
                "pk": "order_id",
                "children": [("ordering", "order_id"),
                             ("Dish", "Dish_ID")] # У вашому SQL Dish_ID FK на order_id
            },
            "ordering": {
                "pk": "ordering_id",
                "children": []
            },
            "Dish": {
                "pk": "Dish_ID",
                "children": []
            }
        }

    def close(self):
        if self.conn:
            self.conn.close()

    def execute(self, sql: str, params: Tuple = (), fetch: bool = False):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch:
                    return list(cur.fetchall())
                else:
                    self.conn.commit()
                    return None
        except Exception:
            self.conn.rollback()
            raise

    def list_tables(self) -> List[str]:
        return list(self.tables.keys())

    def primary_key(self, table: str) -> Optional[str]:
        meta = self.tables.get(table)
        return meta["pk"] if meta else None

    def columns_info(self, table: str) -> List[Dict[str, Any]]:
        sql = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
        """
        rows = self.execute(sql, (table,), fetch=True)
        result = []
        for r in rows:
            name = r["column_name"]
            dtype = r["data_type"]
            nullable = (r["is_nullable"] == "YES")
            default = r["column_default"]
            is_identity = False
            if default and "nextval(" in str(default):
                is_identity = True
            result.append(
                {"name": name, "type": dtype, "nullable": nullable, "identity": is_identity}
            )
        return result

    @staticmethod
    def parse_date(value: str) -> Optional[str]:
        try:
            if len(value) > 10:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                dt = datetime.strptime(value, "%Y-%m-%d")
                return dt.date().isoformat()
        except Exception:
            return None

    @staticmethod
    def cast_value(raw: str, dtype: str) -> Any:
        if raw is None:
            return None
        dtype = dtype.lower()
        if "integer" in dtype or "serial" in dtype:
            return int(raw)
        if dtype in ("real", "double precision", "numeric", "decimal"):
            return float(raw)
        if "timestamp" in dtype or "date" in dtype:
            # Для спрощення повертаємо рядок, PostgreSQL розбереться, якщо формат правильний
            return raw
        return raw

    # ------------- CRUD (стандартні) -------------
    def select_all(self, table: str) -> List[Dict[str, Any]]:
        sql = f'SELECT * FROM public."{table}" ORDER BY 1;'
        return self.execute(sql, fetch=True)

    def select_by_pk(self, table: str, pk: str, pk_val: Any) -> Optional[Dict[str, Any]]:
        sql = f'SELECT * FROM public."{table}" WHERE "{pk}" = %s;'
        rows = self.execute(sql, (pk_val,), fetch=True)
        return rows[0] if rows else None

    def insert(self, table: str, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        cols = []
        vals = []
        placeholders = []
        for k, v in data.items():
            cols.append(f'"{k}"')
            vals.append(v)
            placeholders.append("%s")
        if not cols:
            return False, "Немає даних для вставки."
        sql = f'INSERT INTO public."{table}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)});'
        try:
            self.execute(sql, tuple(vals), fetch=False)
            return True, None
        except Exception as e:
            self.conn.rollback()
            return False, str(e)

    def update(self, table: str, pk: str, pk_val: Any, updates: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if not updates:
            return True, None
        sets = []
        vals = []
        for k, v in updates.items():
            sets.append(f'"{k}" = %s')
            vals.append(v)
        vals.append(pk_val)
        sql = f'UPDATE public."{table}" SET {", ".join(sets)} WHERE "{pk}" = %s;'
        try:
            self.execute(sql, tuple(vals), fetch=False)
            return True, None
        except Exception as e:
            self.conn.rollback()
            return False, str(e)

    def has_child_records(self, table: str, pk: str, pk_val: Any) -> bool:
        meta = self.tables.get(table)
        if not meta:
            return False
        for child_table, child_col in meta["children"]:
            sql = f'SELECT COUNT(*) AS cnt FROM public."{child_table}" WHERE "{child_col}" = %s;'
            rows = self.execute(sql, (pk_val,), fetch=True)
            if rows and rows[0]["cnt"] > 0:
                return True
        return False

    def delete(self, table: str, pk: str, pk_val: Any) -> Tuple[bool, Optional[str]]:
        if self.has_child_records(table, pk, pk_val):
            return False, "Не можна видалити: існують рядки у підлеглих таблицях."
        sql = f'DELETE FROM public."{table}" WHERE "{pk}" = %s;'
        try:
            self.execute(sql, (pk_val,), fetch=False)
            return True, None
        except Exception as e:
            self.conn.rollback()
            return False, str(e)

    # ------------- генерація даних SQL-ом -------------
    def generate_clients(self, n: int):
        sql = """
        INSERT INTO public.client (client_name, phone_number)
        SELECT 
            'Client_' || gs::text,
            '+380' || (100000000 + (random() * 899999999)::bigint)::text
        FROM generate_series(1, %s) AS gs;
        """
        self.execute(sql, (n,), fetch=False)

    def generate_couriers(self, n: int):
        sql = """
        INSERT INTO public.courier (courier_name, transport)
        SELECT 
            'Courier_' || gs::text,
            (ARRAY['Bicycle','Car','Scooter','Foot'])[1 + (random()*3)::int]
        FROM generate_series(1, %s) AS gs;
        """
        self.execute(sql, (n,), fetch=False)

    def generate_orders(self, n: int):
        # Таблиця Order
        sql = """
        INSERT INTO public."Order" (total_amount, order_time)
        SELECT 
            (random() * 2000)::numeric(10,2) + 100,
            timestamp '2024-01-01 00:00:00' + (random() * (interval '365 days'))
        FROM generate_series(1, %s) AS gs;
        """
        self.execute(sql, (n,), fetch=False)

    def generate_ordering(self, n: int):
        # Таблиця ordering зв'язує всіх
        sql = """
        INSERT INTO public.ordering (client_id, courier_id, order_id)
        SELECT
            (SELECT client_id FROM public.client ORDER BY random() LIMIT 1),
            (SELECT courier_id FROM public.courier ORDER BY random() LIMIT 1),
            (SELECT order_id FROM public."Order" ORDER BY random() LIMIT 1)
        FROM generate_series(1, %s) AS gs;
        """
        self.execute(sql, (n,), fetch=False)

    def generate_dishes(self, n: int):

        sql = """
        INSERT INTO public."Dish" ("Dish_ID", total_amount, dish_price)
        SELECT
            o.order_id,
            (o.total_amount)::int, -- припускаємо, що це дублювання суми
            (random() * 500)::int + 50
        FROM public."Order" o
        WHERE NOT EXISTS (SELECT 1 FROM public."Dish" d WHERE d."Dish_ID" = o.order_id)
        ORDER BY random()
        LIMIT %s;
        """
        self.execute(sql, (n,), fetch=False)

    # ------------- 3 пошукові запити (Адаптовано) -------------
    def _timed_query(self, sql: str, params: Tuple = ()) -> Tuple[List[Dict[str, Any]], float]:
        start = time.time()
        rows = self.execute(sql, params, fetch=True)
        elapsed_ms = (time.time() - start) * 1000
        return rows, elapsed_ms

    def search_clients_orders_stats(self, name_pattern: str) -> Tuple[List[Dict[str, Any]], float]:
        """
        1. Клієнти + кількість замовлень + загальна сума витрат (фільтр по імені).
        """
        sql = """
        SELECT c.client_name, 
               c.phone_number,
               COUNT(ord.order_id) as orders_count,
               SUM(o.total_amount) as total_spent
        FROM public.client c
        JOIN public.ordering ord ON c.client_id = ord.client_id
        JOIN public."Order" o ON ord.order_id = o.order_id
        WHERE c.client_name ILIKE %s
        GROUP BY c.client_id, c.client_name
        ORDER BY total_spent DESC;
        """
        return self._timed_query(sql, (f"%{name_pattern}%",))

    def search_couriers_transport_stats(self, transport_type: str) -> Tuple[List[Dict[str, Any]], float]:
        """
        2. Кур'єри певного транспорту + кількість виконаних доставок.
        """
        sql = """
        SELECT cr.courier_name,
               cr.transport,
               COUNT(ord.ordering_id) as deliveries_count
        FROM public.courier cr
        LEFT JOIN public.ordering ord ON cr.courier_id = ord.courier_id
        WHERE cr.transport ILIKE %s
        GROUP BY cr.courier_id, cr.courier_name
        ORDER BY deliveries_count DESC;
        """
        return self._timed_query(sql, (f"%{transport_type}%",))

    def search_dishes_price_range(self, min_price: int, max_price: int) -> Tuple[List[Dict[str, Any]], float]:
        """
        3. Страви в діапазоні цін.
        """
        sql = """
        SELECT d."Dish_ID",
               d.dish_price,
               d.total_amount as order_val_ref
        FROM public."Dish" d
        WHERE d.dish_price BETWEEN %s AND %s
        ORDER BY d.dish_price DESC;
        """
        return self._timed_query(sql, (min_price, max_price))
# model.py
import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, select, inspect, func, text, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from config import DB_URI
from orm_models import Base, Client, Courier, Order, Dish, Ordering


class Model:
    """
    Модель на основі SQLAlchemy ORM.
    Інтерфейси методів збережені (вхід/вихід - словники або примітиви),
    але всередині працює через об'єкти.
    """

    def __init__(self):
        # Створення двигуна (engine)
        self.engine = create_engine(DB_URI, echo=False)
        # Створення фабрики сесій
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Створення таблиць, якщо їх немає (еквівалент SQL CREATE TABLE IF NOT EXISTS)
        # Але оскільки таблиці вже створені скриптом SQL, це просто перевірка відповідності
        Base.metadata.create_all(self.engine)

        # Мапінг: "назва_таблиці_рядком" -> Клас ORM
        self.TableMap = {
            "client": Client,
            "courier": Courier,
            "Order": Order,
            "Dish": Dish,
            "ordering": Ordering
        }

        # Метадані для динамічного аналізу колонок
        self.inspector = inspect(self.engine)

    def close(self):
        self.engine.dispose()

    # --- Допоміжний метод: конвертація ORM об'єкта в dict ---
    def _to_dict(self, obj) -> Dict[str, Any]:
        """Перетворює об'єкт SQLAlchemy у словник для сумісності з Controller."""
        if not obj:
            return None
        return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

    # ------------- Інформація про схему -------------
    def list_tables(self) -> List[str]:
        return list(self.TableMap.keys())

    def primary_key(self, table: str) -> Optional[str]:
        # Отримуємо PK через інспектор SQLAlchemy
        pk_constraint = self.inspector.get_pk_constraint(table)
        if pk_constraint and pk_constraint['constrained_columns']:
            return pk_constraint['constrained_columns'][0]
        # Fallback для специфічних назв у мапінгу
        cls = self.TableMap.get(table)
        if cls:
            return inspect(cls).primary_key[0].name
        return None

    def columns_info(self, table: str) -> List[Dict[str, Any]]:
        """Повертає метадані колонок для генерації меню вставки."""
        # Використовуємо інспектор, щоб дізнатися типи та властивості
        columns = self.inspector.get_columns(table)
        result = []
        for col in columns:
            name = col['name']
            # SQLAlchemy типи перетворюємо у зрозумілі рядки
            dtype = str(col['type']).lower()
            nullable = col['nullable']

            # Визначення identity (autoincrement)
            # У простих випадках PK Integer зазвичай autoincrement
            is_identity = False
            if col.get('autoincrement') is True or (col['primary_key'] and 'int' in dtype):
                # Виняток: Dish.Dish_ID не є autoincrement, бо це FK
                if table == "Dish" and name == "Dish_ID":
                    is_identity = False
                else:
                    is_identity = True

            result.append({
                "name": name,
                "type": dtype,
                "nullable": nullable,
                "identity": is_identity
            })
        return result

    # ------------- Валідація / Кастінг (без змін) -------------
    @staticmethod
    def parse_date(value: str) -> Optional[str]:
        try:
            if len(value) > 10:
                dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                dt = datetime.datetime.strptime(value, "%Y-%m-%d")
                return dt.date().isoformat()
        except Exception:
            return None

    @staticmethod
    def cast_value(raw: str, dtype: str) -> Any:
        if raw is None:
            return None
        dtype = dtype.lower()
        if "int" in dtype or "serial" in dtype:
            return int(raw)
        if dtype in ("real", "double precision", "numeric", "decimal"):
            return float(raw)
        if "timestamp" in dtype or "date" in dtype:
            return raw
        return raw

    # ------------- CRUD через ORM -------------

    def select_all(self, table: str) -> List[Dict[str, Any]]:
        """SELECT * ... за допомогою ORM"""
        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return []

        with self.SessionLocal() as session:
            # Запит: session.query(Model)
            stmt = select(ModelClass)
            result_objs = session.scalars(stmt).all()
            # Перетворення об'єктів у словники
            return [self._to_dict(obj) for obj in result_objs]

    def select_by_pk(self, table: str, pk: str, pk_val: Any) -> Optional[Dict[str, Any]]:
        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return None

        with self.SessionLocal() as session:
            obj = session.get(ModelClass, pk_val)
            return self._to_dict(obj)

    def insert(self, table: str, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return False, "Невідома таблиця."

        try:
            with self.SessionLocal() as session:
                # Створюємо екземпляр класу (екземпляр сутності)
                new_obj = ModelClass(**data)
                session.add(new_obj)
                session.commit()
                return True, None
        except SQLAlchemyError as e:
            return False, str(e)

    def update(self, table: str, pk: str, pk_val: Any, updates: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return False, "Невідома таблиця."

        if not updates:
            return True, None

        try:
            with self.SessionLocal() as session:
                # Отримуємо об'єкт
                obj = session.get(ModelClass, pk_val)
                if not obj:
                    return False, "Запис не знайдено."

                # Оновлюємо атрибути об'єкта
                for key, value in updates.items():
                    if hasattr(obj, key):
                        setattr(obj, key, value)

                session.commit()
                return True, None
        except SQLAlchemyError as e:
            return False, str(e)

    def has_child_records(self, table: str, pk: str, pk_val: Any) -> bool:
        """
        Перевірка зовнішніх зв'язків засобами ORM.
        Перевіряємо, чи є пов'язані об'єкти у списках (relationships).
        """
        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return False

        with self.SessionLocal() as session:
            obj = session.get(ModelClass, pk_val)
            if not obj:
                return False

            # Перевірка специфічних зв'язків
            if table == "client" or table == "courier" or table == "Order":
                # Перевіряємо список orderings (1:M)
                # Якщо список не порожній, значить є залежні записи
                if hasattr(obj, "orderings") and obj.orderings:
                    return True

            if table == "Order":
                # Перевірка Dish (1:1)
                if hasattr(obj, "dish") and obj.dish:
                    return True

            return False

    def delete(self, table: str, pk: str, pk_val: Any) -> Tuple[bool, Optional[str]]:
        # Контроль зовнішніх зв'язків перед видаленням
        if self.has_child_records(table, pk, pk_val):
            return False, "Не можна видалити: існують залежні записи (контроль ORM)."

        ModelClass = self.TableMap.get(table)
        if not ModelClass:
            return False, "Невідома таблиця."

        try:
            with self.SessionLocal() as session:
                obj = session.get(ModelClass, pk_val)
                if obj:
                    session.delete(obj)
                    session.commit()
                    return True, None
                else:
                    return False, "Запис не знайдено."
        except SQLAlchemyError as e:
            return False, str(e)

    # ------------- Генерація даних (ORM підхід) -------------

    def generate_clients(self, n: int):
        with self.SessionLocal() as session:
            import random
            for i in range(n):
                name = f"Client_ORM_{random.randint(1000, 9999)}"
                phone = f"+380{random.randint(100000000, 999999999)}"
                # Створення об'єкта
                client = Client(client_name=name, phone_number=phone)
                session.add(client)
            session.commit()

    def generate_couriers(self, n: int):
        with self.SessionLocal() as session:
            import random
            transports = ['Car', 'Bicycle', 'Scooter', 'Foot']
            for i in range(n):
                name = f"Courier_ORM_{random.randint(1000, 9999)}"
                trans = random.choice(transports)
                courier = Courier(courier_name=name, transport=trans)
                session.add(courier)
            session.commit()

    def generate_orders(self, n: int):
        with self.SessionLocal() as session:
            import random
            for i in range(n):
                amount = round(random.uniform(100, 2000), 2)
                # Час
                t = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))
                order = Order(total_amount=amount, order_time=t)
                session.add(order)
            session.commit()

    def generate_ordering(self, n: int):
        # M:M зв'язок. Беремо випадкових клієнтів, кур'єрів і замовлення
        with self.SessionLocal() as session:
            import random
            # Отримуємо всі ID (scalars() повертає плоский список значень)
            c_ids = session.scalars(select(Client.client_id)).all()
            cr_ids = session.scalars(select(Courier.courier_id)).all()
            o_ids = session.scalars(select(Order.order_id)).all()

            if not c_ids or not cr_ids or not o_ids:
                return  # Немає з чого генерувати

            for i in range(n):
                obj = Ordering(
                    client_id=random.choice(c_ids),
                    courier_id=random.choice(cr_ids),
                    order_id=random.choice(o_ids)
                )
                session.add(obj)
            session.commit()

    def generate_dishes(self, n: int):
        # 1:1 Dish -> Order
        # Треба знайти Order, у яких ще немає Dish
        with self.SessionLocal() as session:
            import random
            # Знаходимо order_id, яких немає в таблиці Dish
            # (ORM еквівалент LEFT JOIN ... WHERE IS NULL або NOT EXISTS)
            subq = select(Dish.dish_id)
            stmt = select(Order).where(Order.order_id.not_in(subq)).limit(n)
            orders_without_dish = session.scalars(stmt).all()

            for o in orders_without_dish:
                d = Dish(
                    dish_id=o.order_id,  # PK = FK
                    total_amount=int(o.total_amount),
                    dish_price=random.randint(50, 500)
                )
                session.add(d)
            session.commit()

    # ------------- Пошукові запити через ORM -------------

    def _timed_query(self, stmt) -> Tuple[List[Dict[str, Any]], float]:
        """Виконує ORM-запит із заміром часу"""
        start = datetime.datetime.now()
        with self.SessionLocal() as session:
            # Виконання запиту
            result = session.execute(stmt).all()

            # Обробка результату (result - це список рядків/кортежів)
            # Перетворимо в список словників
            rows = []
            for row in result:
                # row._mapping перетворює рядок результату в dict-like об'єкт
                rows.append(dict(row._mapping))

        elapsed = (datetime.datetime.now() - start).total_seconds() * 1000
        return rows, elapsed

    def search_clients_orders_stats(self, name_pattern: str) -> Tuple[List[Dict[str, Any]], float]:
        """ORM запит: Клієнти + кількість замовлень + сума"""
        # Еквівалент: SELECT name, phone, count, sum FROM client JOIN ordering JOIN Order ...
        stmt = (
            select(
                Client.client_name,
                Client.phone_number,
                func.count(Ordering.order_id).label("orders_count"),
                func.sum(Order.total_amount).label("total_spent")
            )
            .join(Ordering, Client.client_id == Ordering.client_id)
            .join(Order, Ordering.order_id == Order.order_id)
            .where(Client.client_name.ilike(f"%{name_pattern}%"))
            .group_by(Client.client_id, Client.client_name)
            .order_by(desc("total_spent"))
        )
        return self._timed_query(stmt)

    def search_couriers_transport_stats(self, transport_type: str) -> Tuple[List[Dict[str, Any]], float]:
        """ORM запит: Кур'єри + кількість доставок"""
        stmt = (
            select(
                Courier.courier_name,
                Courier.transport,
                func.count(Ordering.ordering_id).label("deliveries_count")
            )
            .outerjoin(Ordering, Courier.courier_id == Ordering.courier_id)
            .where(Courier.transport.ilike(f"%{transport_type}%"))
            .group_by(Courier.courier_id, Courier.courier_name)
            .order_by(desc("deliveries_count"))
        )
        return self._timed_query(stmt)

    def search_dishes_price_range(self, min_price: int, max_price: int) -> Tuple[List[Dict[str, Any]], float]:
        """ORM запит: Страви в діапазоні цін"""
        stmt = (
            select(
                Dish.dish_id.label("Dish_ID"),
                Dish.dish_price,
                Dish.total_amount.label("order_val_ref")
            )
            .where(Dish.dish_price.between(min_price, max_price))
            .order_by(desc(Dish.dish_price))
        )
        return self._timed_query(stmt)
# orm_models.py
from typing import List, Optional
from sqlalchemy import Integer, String, Numeric, ForeignKey, TIMESTAMP, MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# Базовий клас
class Base(DeclarativeBase):
    pass


# 1. Клієнт
class Client(Base):
    __tablename__ = 'client'

    client_id: Mapped[int] = mapped_column(primary_key=True)
    client_name: Mapped[str] = mapped_column(String(255))
    phone_number: Mapped[Optional[str]] = mapped_column(String(50))

    # Зв'язок 1:M з Ordering (Один клієнт -> багато записів у таблиці зв'язків)
    orderings: Mapped[List["Ordering"]] = relationship(back_populates="client", cascade="all, delete-orphan")


# 2. Кур'єр
class Courier(Base):
    __tablename__ = 'courier'

    courier_id: Mapped[int] = mapped_column(primary_key=True)
    courier_name: Mapped[str] = mapped_column(String(255))
    transport: Mapped[Optional[str]] = mapped_column(String(100))

    # Зв'язок 1:M з Ordering
    orderings: Mapped[List["Ordering"]] = relationship(back_populates="courier", cascade="all, delete-orphan")


# 3. Замовлення ("Order" - зарезервоване слово, тому лапки в назві таблиці)
class Order(Base):
    __tablename__ = 'Order'  # Важливо: точна назва як у БД

    order_id: Mapped[int] = mapped_column(primary_key=True)
    total_amount: Mapped[float] = mapped_column(Numeric(10, 2))
    order_time: Mapped[str] = mapped_column(TIMESTAMP)

    # Зв'язок 1:M з Ordering
    orderings: Mapped[List["Ordering"]] = relationship(back_populates="order", cascade="all, delete-orphan")

    # Зв'язок 1:1 з Dish (uselist=False робить це "один до одного")
    dish: Mapped[Optional["Dish"]] = relationship(back_populates="order", uselist=False, cascade="all, delete-orphan")


# 4. Страва
class Dish(Base):
    __tablename__ = 'Dish'

    # PK тут є також FK на Order (Зв'язок 1:1)
    dish_id: Mapped[int] = mapped_column("Dish_ID", ForeignKey("Order.order_id"), primary_key=True)
    total_amount: Mapped[int] = mapped_column(Integer)
    dish_price: Mapped[int] = mapped_column(Integer)

    # Зворотній зв'язок до Order
    order: Mapped["Order"] = relationship(back_populates="dish")


# 5. Проміжна таблиця (Ordering) - реалізує зв'язок M:M між Client, Courier, Order
class Ordering(Base):
    __tablename__ = 'ordering'

    ordering_id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("client.client_id"))
    courier_id: Mapped[int] = mapped_column(ForeignKey("courier.courier_id"))
    order_id: Mapped[int] = mapped_column(ForeignKey("Order.order_id"))

    # ORM-зв'язки для доступу до об'єктів
    client: Mapped["Client"] = relationship(back_populates="orderings")
    courier: Mapped["Courier"] = relationship(back_populates="orderings")
    order: Mapped["Order"] = relationship(back_populates="orderings")
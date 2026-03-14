from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime


class AddressInfo(BaseModel):
    full: Optional[str] = Field(
        None, description="Полный адрес объекта строкой, как указано в объявлении."
    )
    district: Optional[str] = Field(
        None,
        description="Район города (например: Даниловский, Хамовники). Только название района.",
    )
    metro_station: Optional[str] = Field(
        None,
        description="Ближайшая станция метро (Только название станции, без слова 'метро').",
    )
    okrug: Optional[str] = Field(
        None,
        description="Округ Москвы (ТОЛЬКО одна из 12 аббревиатур: ЦАО, САО, СВАО, ВАО, ЮВАО, ЮАО, ЮЗАО, ЗАО, СЗАО, ЗелАО, Троицкий, Новомосковский). Если нет точного совпадения, верните null.",
    )


class FloorInfo(BaseModel):
    current: Optional[int] = Field(
        None,
        description="Текущий этаж, на котором находится квартира (строго целое число).",
    )
    all: Optional[int] = Field(
        None, description="Всего этажей в здании (строго целое число)."
    )


class ParsedAdData(BaseModel):
    # Base fields from schema
    price: Optional[int] = Field(
        None,
        description="Цена в рублях. СТРОГО целое число (например, 20000000). Без пробелов, запятых, точек и слова 'рублей'.",
    )
    title: Optional[str] = Field(
        None, description="Заголовок объявления строго как на сайте."
    )
    address: Optional[AddressInfo] = None
    description: Optional[str] = Field(
        None, description="Полное текстовое описание объявления со всеми подробностями."
    )
    price_per_m2: Optional[int] = Field(
        None,
        description="Цена за квадратный метр в рублях. СТРОГО целое число. Без пробелов и символов валют.",
    )
    area: Optional[float] = Field(
        None,
        description="Общая площадь в м². СТРОГО число (например 45.5), без букв 'м2' или 'кв.м'.",
    )
    construction_year: Optional[int] = Field(
        None,
        description="Год постройки здания. СТРОГО целое число (например 2021). Если неизвестно, null.",
    )
    days_in_exposition: Optional[int] = Field(
        None,
        description="Количество дней в экспозиции (на сайте). СТРОГО целое число. Если нет, null.",
    )
    floor_info: Optional[FloorInfo] = None
    housing_type: Optional[str] = Field(
        None,
        description="Тип жилья. Допустимые значения: 'Вторичка', 'Новостройка', 'Вторичка Апартаменты', 'Новостройка Апартаменты'.",
    )
    metro_walk_time: Optional[int] = Field(
        None,
        description="Время пешком до метро в минутах. СТРОГО целое число. Никаких слов 'мин' или 'минут'.",
    )
    renovation: Optional[str] = Field(
        None,
        description="Тип ремонта. Одно из: 'Без ремонта', 'Косметический', 'Евроремонт', 'Дизайнерский', 'Предчистовая отделка'.",
    )
    rooms: Optional[int] = Field(
        None,
        description="Количество комнат. СТРОГО целое число. Для студий ставьте 0. Никаких слов 'комн'.",
    )
    total_views: Optional[int] = Field(
        None,
        description="Всего просмотров объявления. СТРОГО целое число. Если нет данных, null.",
    )
    unique_views: Optional[int] = Field(
        None,
        description="Уникальных просмотров (за сегодня или всего). СТРОГО одно целое число. Если нет, null.",
    )
    cian_id: Optional[str] = Field(
        None,
        description="ID объявления на Циан. СТРОГО строка с цифрами (например '312533860').",
    )
    publish_date: Optional[str] = Field(
        None,
        description="Дата публикации. Формат строго YYYY-MM-DD (например 2023-10-25). Без дополнительных слов.",
    )

    # Internal fields
    url: str
    parsed_at: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        description="Дата и время парсинга. Firecrawl НЕ ДОЛЖЕН извлекать или перезаписывать это поле.",
    )

    def to_row(self) -> List[Any]:
        """Convert model to a flat row matching the requested PARSED columns"""
        addr_full = self.address.full if self.address and self.address.full else ""
        district = (
            self.address.district if self.address and self.address.district else ""
        )
        metro_station = (
            self.address.metro_station
            if self.address and self.address.metro_station
            else ""
        )
        okrug = self.address.okrug if self.address and self.address.okrug else ""

        floor_str = ""
        if self.floor_info:
            curr = self.floor_info.current or ""
            total = self.floor_info.all or ""
            # Prefix with an apostrophe to force Google Sheets to read it as a string
            # and prevent formatting 13/17 as magical dates or weird numbers like 46367
            floor_str = f"'{curr}/{total}" if curr or total else ""

        # Requested columns:
        # url, publish_date, price, title, address, description, price_per_m2, area, construction_year, days_in_exposition,
        # district, floor_info, housing_type, metro_station, metro_walk_time, okrug, renovation, rooms, total_views, unique_views, cian_id, parsed_at

        return [
            self.url,
            self.publish_date or "",
            self.price or "",
            self.title or "",
            addr_full,
            self.description or "",
            self.price_per_m2 if self.price_per_m2 is not None else "",
            self.area if self.area is not None else "",
            self.construction_year if self.construction_year is not None else "",
            self.days_in_exposition if self.days_in_exposition is not None else "",
            district,
            floor_str,
            self.housing_type or "",
            metro_station,
            self.metro_walk_time if self.metro_walk_time is not None else "",
            okrug,
            self.renovation or "",
            self.rooms if self.rooms is not None else "",
            self.total_views if self.total_views is not None else "",
            self.unique_views if self.unique_views is not None else "",
            self.cian_id or "",
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # Forcibly overwrite whatever LLM gave with actual execution time
        ]

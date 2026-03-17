"""
services.parser_cian.models - Pydantic models for Cian real estate ads

Модели данных специфичные для парсера Cian.
Абстрактны от способа хранения (Google Sheets и т.д).
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any, Literal, Union
from datetime import datetime


class AddressInfo(BaseModel):
    """Информация об адресе объекта"""
    
    full: Optional[str] = Field(
        None, 
        description="Полный адрес объекта строкой, как указано в объявлении."
    )
    district: Optional[str] = Field(
        None,
        description="Район города (например: Даниловский, Хамовники).",
    )
    metro_station: Optional[str] = Field(
        None,
        description="Ближайшая станция метро (Только название станции).",
    )
    okrug: Optional[str] = Field(
        None,
        description="Округ Москвы (аббревиатура).",
    )


class FloorInfo(BaseModel):
    """Информация об этажах"""
    
    current: Optional[int] = Field(
        None,
        description="Текущий этаж.",
    )
    all: Optional[int] = Field(
        None, 
        description="Всего этажей в здании."
    )


class PriceHistoryEntry(BaseModel):
    """Запись истории изменения цены"""
    
    date: str = Field(
        ...,
        description="Дата изменения цены (например: '10 мар 2026')"
    )
    price: int = Field(
        ...,
        description="Цена в рублях на эту дату"
    )
    change_amount: int = Field(
        0,
        description="На сколько изменилась цена (0 для первой публикации)"
    )
    change_type: Literal["initial", "decrease", "increase"] = Field(
        "initial",
        description="Тип изменения: initial (первая цена), decrease (снижение), increase (повышение)"
    )


class ParsedAdData(BaseModel):
    """
    Распарсенные данные объявления недвижимости с Cian.
    
    Это сырая модель, которая выходит из парсера Firecrawl.
    Не должна содержать логику преобразования для Google Sheets.
    """

    # Основные поля
    url: str = Field(
        ..., 
        description="URL объявления на cian.ru"
    )
    cian_id: Optional[str] = Field(
        None, 
        description="ID объявления в системе Cian"
    )
    
    # Цена и финансовые показатели
    price: Optional[int] = Field(
        None,
        description="Цена в рублях (целое число, без пробелов).",
    )
    price_per_m2: Optional[int] = Field(
        None,
        description="Цена за квадратный метр в рублях.",
    )

    # Основная информация
    title: Optional[str] = Field(
        None, 
        description="Заголовок объявления."
    )
    description: Optional[str] = Field(
        None, 
        description="Полное описание объявления."
    )

    # Адрес
    address: Optional[AddressInfo] = Field(
        None,
        description="Информация об адресе."
    )

    # Параметры помещения
    area: Optional[float] = Field(
        None,
        description="Площадь в кв.м.",
    )
    rooms: Optional[int] = Field(
        None,
        description="Количество комнат.",
    )
    housing_type: Optional[str] = Field(
        None,
        description="Тип жилья (квартира, дом, студия и т.д).",
    )

    # Информация об этажах
    floor_info: Optional[FloorInfo] = Field(
        None,
        description="Информация о текущем и общем этажах."
    )

    # Дом и окрестности
    construction_year: Optional[int] = Field(
        None,
        description="Год постройки дома.",
    )
    renovation: Optional[str] = Field(
        None,
        description="Статус ремонта.",
    )
    metro_walk_time: Optional[int] = Field(
        None,
        description="Время пешком до метро в минутах.",
    )

    # Статистика
    publish_date: Optional[str] = Field(
        None,
        description="Дата публикации объявления (формат: YYYY-MM-DD).",
    )
    days_in_exposition: Optional[int] = Field(
        None,
        description="Дней объявление в каталоге.",
    )
    total_views: Optional[int] = Field(
        None,
        description="Всего просмотров.",
    )
    unique_views: Optional[int] = Field(
        None,
        description="Уникальных просмотров (сегодня).",
    )

    # Валидаторы для преобразования "soon" в None
    @field_validator('days_in_exposition', 'total_views', 'unique_views', mode='before')
    @classmethod
    def convert_soon_to_none(cls, v):
        if v == "soon":
            return None
        return v

    # История цен
    price_history: Optional[List[PriceHistoryEntry]] = Field(
        None,
        description="История изменения цены объявления (если есть).",
    )

    # Служебные поля
    parsed_at: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        description="Когда данные были распарсены.",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "url": "https://www.cian.ru/sale/flat/312533860/",
                "cian_id": "312533860",
                "price": 20000000,
                "title": "2-комнатная квартира, 45 м², этаж 3/5",
                "address": {
                    "full": "Москва, ул. Ленина, д. 10",
                    "district": "Даниловский",
                    "metro_station": "Октябрьская",
                    "okrug": "ЮАО"
                },
                "area": 45.5,
                "rooms": 2,
                "parsed_at": "2024-01-15 14:30:00"
            }
        }


# ========================
# Адаптеры для Google Sheets
# ========================


def parse_to_sheets_row(data: ParsedAdData) -> List[Any]:
    """
    Адаптер: преобразует ParsedAdData в список значений для Google Sheets.
    
    Порядок полей соответствует колонкам в табличке "RESULTS".
    
    Args:
        data: Распарсенные данные объявления
        
    Returns:
        Список значений для одной строки таблицы
    """
    
    # Распаковка вложенных объектов
    addr_full = data.address.full if data.address and data.address.full else ""
    district = data.address.district if data.address and data.address.district else ""
    metro_station = (
        data.address.metro_station
        if data.address and data.address.metro_station
        else ""
    )
    okrug = data.address.okrug if data.address and data.address.okrug else ""

    # Этажи в формате "3/5"
    floor_str = ""
    if data.floor_info:
        curr = data.floor_info.current or ""
        total = data.floor_info.all or ""
        floor_str = f"{curr}/{total}" if curr and total else (str(curr) if curr else "")

    # Собираем строку согласно схеме Google Sheets
    return [
        data.url or "",                                    # A: URL
        data.publish_date or "",                           # B: Дата публикации
        data.price or "",                                  # C: Цена
        data.title or "",                                  # D: Заголовок
        addr_full,                                         # E: Полный адрес
        data.description or "",                            # F: Описание
        data.price_per_m2 if data.price_per_m2 else "",   # G: Цена за м²
        data.area if data.area else "",                    # H: Площадь
        data.construction_year or "",                      # I: Год постройки
        data.days_in_exposition or "",                     # J: Дней в каталоге
        district,                                          # K: Район
        floor_str,                                         # L: Этажи
        data.housing_type or "",                           # M: Тип жилья
        metro_station,                                     # N: Метро
        data.metro_walk_time or "",                        # O: Время до метро
        okrug,                                             # P: Округ
        data.renovation or "",                             # Q: Ремонт
        data.rooms or "",                                  # R: Комнаты
        data.total_views or "",                            # S: Всего просмотров
        data.unique_views or "",                           # T: Уникальных просмотров
        data.cian_id or "",                                # U: ID Cian
        data.parsed_at,                                    # V: Время парсинга
    ]

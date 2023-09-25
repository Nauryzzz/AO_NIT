SELECT
    sgf.ID_SK_FAMILY_QUALITY2,
    FULL_KATO_NAME,
    sgf.KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    toInt32OrZero(KATO_4) AS KATO_4,
    (
        case
            when FAMILY_CAT = 'A' then 'А - благополучный уровень семьи (средний и выше)'
            when FAMILY_CAT = 'B' then 'В - удовлетворительный уровень семьи (ниже среднего)'
            when FAMILY_CAT = 'C' then 'С - Неблагополучный уровень семьи (необходим мониторинг) '
            when FAMILY_CAT = 'D' then 'D - Кризисный уровень семьи (требуется помощь)'
            when FAMILY_CAT = 'E' then 'E - Экстренный уровень семьи (требуется срочная помощь)'
        end
    ) FAMILY_CAT,
    REPLACE(
        filtr1,
        'Уровень среднедушевого дохода в семье – ',
        ''
    ) AS filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    (
        case
            when filtr7 = 'В семье есть земельный участок под сельхозземельные участки (более 1 Га)' then 'В семье есть сельхозземельные участки (более 1 Га)'
            when filtr7 = 'В семье есть земельный участок под индивидуальное жилищное строительство' then 'В семье есть земельный участок под ИЖС'
            when filtr7 = 'В семье есть земельный участок под индивидуальное жилищное строительство и земельный участок под сельхозземельные участки (более 1 Га)' then 'В семье есть земельный участок под ИЖС и сельхозземельные участки (более 1 Га)'
            else filtr7
        end
    ) as filtr7,
    filtr8,
    filtr9,
    filtr10,
    ID_SK_FAMILY_QUALITY22,
    (
        case
            when filtr11 = 'В семье есть три несовершеннолетних детей и учащейся молодежи до 23 лет' then 'В семье есть трое несовершеннолетних детей и учащейся молодежи до 23 лет'
            when filtr11 = 'В семье есть один несовершеннолетнии ребенок или учащейся молодеж до 23 лет' then 'В семье есть один несовершеннолетний ребенок или учащейся молодеж до 23 лет'
            else filtr11
        end
    ) as filtr11,
    (
        case
            when filtr12 = 'В семье отсутсвуют лица с инвалидностью (1 и 2 группы)' then 'В семье нет лиц с инвалидностью (1 и 2 группы)'
            else filtr12
        end
    ) as filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    (
        case
            when filtr20 = 'Семьи, в которых у взрослых членов семьи нет профессионального и среднеспециального образование' then 'В семье нет профессионального и среднеспециального образование'
            when filtr20 = 'Семьи, в которых у взрослых членов семьи есть профессиональное и среднеспециальное образование' then 'В семье есть профессиональное и среднеспециальное образование'
            else filtr20
        end
    ) as filtr20,
    filtr21,
    filtr22,
    filtr23,
    (
        case
            when filtr24 = 'Все совершеннолетние члены семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 'Члены семьи имеют просроченную задолженность по кредитам'
            when filtr24 = 'Один совершеннолетнии член семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 'Один член семьи имеет просроченную задолженность по кредитам'
            when filtr24 = 'Семья не имеет задолженности по кредитам больше 90 дней и свыше 1000 тенге' then 'Семья не имеет задолженности по кредитам'
            else filtr24
        end
    ) as filtr24,
    filtr25,
    filtr26,
    filtr27,
    filtr28,
    filtr29,
    filtr30,
    filtr31,
    filtr32,
    IF(
        fa.FAMILY_ID = 0,
        'Нет обращения',
        'Есть обращение'
    ) filtr33,
    filtr34,
    
    IF(
    	set8.filtr35 = 1,
    	'В семье есть ребенок, обеспеченный бесплатным питанием',
    	'В семье нет ребенка обеспеченного бесплатным питанием'
    ) as filtr35,
    
    IF(
    	set8.filtr36 = 1,
    	'В семье есть ребенок, обеспеченный бесплатным подвозом до школы',
    	'В семье нет ребенка обеспеченного бесплатным подвозом до школы'
    ) as filtr36,
    
    IF(
    	set8.filtr37 = 1,
    	'В семье есть ребенок с инвалидностью, который учится в школе',
    	'В семье нет ребенка с инвалидностью, который учится в школе'
    ) as filtr37,
    
    IF(
    	set8.filtr38 = 1,
    	'Семьи, в которых у взрослого члена семьи есть зависимость от ПАВ',
    	'Семьи, в которых нет взрослого члена семьи зависимого от ПАВ'
    ) as filtr38,
    
    IF(
    	set8.filtr39 = 1,
    	'Семьи, в которых один из членов семьи зависим от ПАВ с принудительным решением труда',
    	'Семьи, в которых нет членов семьи зависимых от ПАВ с принудительным решением труда'
    ) as filtr39,
    
    count_iin,
    KATO_42,
    value_rayon,
    max_value,
    Rating,
    max_reg
FROM
    SOC_KARTA.SEGMENTATION_GROUP_FINAL3 AS sgf
    LEFT JOIN (
        SELECT
            DISTINCT FAMILY_ID
        FROM
            SOC_KARTA.FAMILY_APPEALS3
    ) AS fa ON sgf.ID_SK_FAMILY_QUALITY2 = toString(fa.FAMILY_ID)
    LEFT JOIN (
        select
            ID_SK_FAMILY_QUALITY,
            (
                case
                    when SUM(DETI8) > 0
                    and SUM(CNTDETI8) > 0 then 'В семье есть дети от 8 до 14 лет включительно не прикрепленые к школе'
                    when SUM(DETI8) < 1
                    and SUM(CNTDETI8) > 0 then 'В семье есть дети от 8 до 14 лет включительно прикрепленые к школе'
                    when SUM(CNTDETI8) < 1 then 'В семье нет детей от 8 до 14 лет включительно'
                    else 'netu'
                end
            ) as filtr34,
            if(
                MAX(IS_VILLAGE_IIN) > 0,
                'Семья живет в селе',
                'Семья живет в городе'
            ) as filtr31,
            if(
                sum(SSD_PATIENTS) > 0,
                'В Семье есть социально значимые заболевания',
                'В Семье нет социально значимых заболевании'
            ) as filtr32
        from
            (
                SELECT
                    ID_SK_FAMILY_QUALITY,
                    IS_VILLAGE_IIN,
                    SSD_PATIENTS,
                    if(
                        BIRTH_DATE_IIN is null,
                        -1,
                        toInt64(
                            datediff(
                                'day',
                                toDateOrZero(BIRTH_DATE_IIN),
                                toDate('2022-06-01 00:00:00')
                            ) / 365
                        )
                    ) as raz,
                    if(
                        raz < 15
                        and raz > 7
                        and IS_SCHOOL_PRIKR_IIN == 0,
                        1,
                        0
                    ) as DETI8,
                    if(
                        raz < 15
                        and raz > 7,
                        1,
                        0
                    ) as CNTDETI8
                FROM
                    SOC_KARTA.SK_FAMILY_QUALITY_IIN
            )
        group by
            ID_SK_FAMILY_QUALITY
    ) as set6 on sgf.ID_SK_FAMILY_QUALITY2 = toString(set6.ID_SK_FAMILY_QUALITY)
    left join (
        SELECT
            KATO_2_NAME,
            count(distinct(ID_SK_FAMILY_QUALITY2)) max_reg
        FROM
            SOC_KARTA.SK_FAMILY_VITRINA_KATO
        group by
            KATO_2_NAME
    ) as set7 on sgf.KATO_2_NAME = set7.KATO_2_NAME
    left join 
    	SOC_KARTA.filtr35_39 as set8 on sgf.ID_SK_FAMILY_QUALITY2 = set8.SK_FAMILY_ID;
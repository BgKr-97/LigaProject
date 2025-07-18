-- ===================================================================
-- 1. Создание схем staging, core и ENUM-типов для полей
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- ===================================================================
-- 2. ENUM для пола, образования, типа занятости, семейного статуса
-- ===================================================================
CREATE TYPE gender_enum AS ENUM ('Мужчина','Женщина');
CREATE TYPE education_enum AS ENUM ('Начальное','Среднее','Высшее','Два высших');
CREATE TYPE employment_enum AS ENUM ('Безработный','ИП','Самозанятый','Работает по найму');
CREATE TYPE marital_enum AS ENUM ('В браке','Не в браке');

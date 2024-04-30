DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Проверка существования таблицы и удаление, если она существует
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'calculations') THEN
        -- Удаление таблицы
        EXECUTE 'DROP TABLE calculations';
    END IF;
END $$;

-- Создание таблицы
CREATE TABLE calculations (
    id SERIAL PRIMARY KEY,
    slide INT,
    accum_pulses INT,
    delay_pts INT,
    pulse_width_pts INT,
    end_offset_pts INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	UNIQUE(slide,accum_pulses,delay_pts,pulse_width_pts,end_offset_pts)
);

-- Проверка существования conditions и удаление, если она существует
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'conditions') THEN
        -- Удаление таблицы
        EXECUTE 'DROP TABLE conditions';
    END IF;
END $$;

-- Создание таблицы
CREATE TABLE conditions (
    id SERIAL PRIMARY KEY,
    substance VARCHAR(30),
    concentration float,
    filter_type VARCHAR(20),
    filter_power INT,
    end_offset_pts INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Проверка существования experiment и удаление, если она существует
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'experiment') THEN
        -- Удаление таблицы
        EXECUTE 'DROP TABLE experiment';
    END IF;
END $$;

-- Создание таблицы
CREATE TABLE experiment (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    description VARCHAR(250),
    calc_id INTEGER REFERENCES calculations(id),
	cond_id INTEGER REFERENCES conditions(id),
    reper_file_link VARCHAR(150),
    analyt_file_link VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Проверка существования measurements и удаление, если она существует
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'measurements') THEN
        -- Удаление таблицы
        EXECUTE 'DROP TABLE measurements';
    END IF;
END $$;

-- Создание таблицы
CREATE TABLE measurements (
    id SERIAL PRIMARY KEY,
	exp_id INTEGER REFERENCES experiment(id),
    start_time TIME,
	step INT,
	delay_pulses INT,
	reper_energy FLOAT,
    analyt_energy FLOAT,
    rep_analyt_ratio FLOAT
);
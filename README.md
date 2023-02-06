# Bike_Index_Challenge



pip install requests on master node.

psql -U test


CREATE TABLE bike_data (
  date_stolen bigint,
  description text,
  frame_colors text[],
  frame_model text,
  id integer,
  is_stock_img boolean,
  large_img text,
  location_found text,
  manufacturer_name text,
  external_id text,
  registry_name text,
  registry_url text,
  serial text,
  status text,
  stolen boolean,
  stolen_coordinates double precision[],
  stolen_location text,
  thumb text,
  title text,
  url text,
  year integer
);
CREATE TABLE "m_item" (
	"item_id"	SERIAL NOT NULL,
	"name"	VARCHAR(100) NOT NULL,
	"desc"	VARCHAR(100)
);

CREATE TABLE "t_sale" (
	"sale_id"	SERIAL NOT NULL,
	"item_id"	INTEGER NOT NULL,
	"qty"	INTEGER NOT NULL,
	"payed"	VARCHAR(100) NOT NULL,
	"sale_date" TIMESTAMP WITH TIME ZONE
);


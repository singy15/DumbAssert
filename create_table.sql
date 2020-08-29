CREATE TABLE "m_item" (
	"item_id"	INTEGER NOT NULL,
	"name"	TEXT NOT NULL,
	"desc"	TEXT,
	PRIMARY KEY("item_id" AUTOINCREMENT)
);

CREATE TABLE "t_sale" (
	"sale_id"	INTEGER NOT NULL,
	"item_id"	INTEGER NOT NULL,
	"qty"	INTEGER NOT NULL,
	"payed"	TEXT NOT NULL,
	PRIMARY KEY("sale_id" AUTOINCREMENT)
);


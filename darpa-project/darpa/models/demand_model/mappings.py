import numpy as np

# --------------------------
# High Frequency Survey Data
# --------------------------
ss_hfs_options = {
    "state": {
        81: "Warrap",
        82: "Northern Bahr el Ghazal",
        83: "Western Bahr el Ghazal",
        84: "Lakes",
        91: "Western Equatoria",
        92: "Central Equatoria",
        93: "Eastern Equatoria",
    },
    "code": {
        "Warrap": 81,
        "Northern Bahr el Ghazal": 82,
        "Western Bahr el Ghazal": 83,
        "Lakes": 84,
        "Western Equatoria": 91,
        "Central Equatoria": 92,
        "Eastern Equatoria": 93,
    },
}


ss_state_spelling = {
    "state": {
        "Northern Bahr El Ghazal": "Northern Bahr el Ghazal",
        "Western Bahr El Ghazal": "Western Bahr el Ghazal",
    }
}


state_code_to_rasterize = {
    "Central Equatoria": 1.0,
    "Eastern Equatoria": 2.0,
    "Jonglei": 3.0,
    "Lakes": 4.0,
    "Northern Bahr el Ghazal": 5.0,
    "Unity": 6.0,
    "Upper Nile": 7.0,
    "Warrap": 8.0,
    "Western Bahr el Ghazal": 9.0,
    "Western Equatoria": 10.0,
}

county_code_to_rasterize = {
    "Abiemnhom": 1,
    "Abyei": 2,
    "Akobo": 3,
    "Aweil Centre": 4,
    "Aweil East": 5,
    "Aweil North": 6,
    "Aweil South": 7,
    "Aweil West": 8,
    "Awerial": 9,
    "Ayod": 10,
    "Baliet": 11,
    "Bor South": 12,
    "Budi": 13,
    "Cueibet": 14,
    "Duk": 15,
    "Ezo": 16,
    "Fangak": 17,
    "Fashoda": 18,
    "Gogrial East": 19,
    "Gogrial West": 20,
    "Guit": 21,
    "Ibba": 22,
    "Ikotos": 23,
    "Juba": 24,
    "Jur River": 25,
    "Kajo-Keji": 26,
    "Kapoeta East": 27,
    "Kapoeta North": 28,
    "Kapoeta South": 29,
    "Khorflus": 30,
    "Koch": 31,
    "Lainya": 32,
    "Leer": 33,
    "Longochuk": 34,
    "Lopa": 35,
    "Luakpiny/Nasir": 36,
    "Maban": 37,
    "Magwi": 38,
    "Maiwut": 39,
    "Malakal": 40,
    "Manyo": 41,
    "Maridi": 42,
    "Mayendit": 43,
    "Mayom": 44,
    "Melut": 45,
    "Morobo": 46,
    "Mundri East": 47,
    "Mundri West": 48,
    "Mvolo": 49,
    "Nagero": 50,
    "Nyirol": 51,
    "Nzara": 52,
    "Panyijar": 53,
    "Panyikang": 54,
    "Pariang": 55,
    "Pibor": 56,
    "Pochalla": 57,
    "Raga": 58,
    "Renk": 59,
    "Rubkona": 60,
    "Rumbek Centre": 61,
    "Rumbek East": 62,
    "Rumbek North": 63,
    "Tambura": 64,
    "Terekeka": 65,
    "Tonj East": 66,
    "Tonj North": 67,
    "Tonj South": 68,
    "Torit": 69,
    "Twic": 70,
    "Twic East": 71,
    "Ulang": 72,
    "Uror": 73,
    "Wau": 74,
    "Wulu": 75,
    "Yambio": 76,
    "Yei": 77,
    "Yirol East": 78,
    "Yirol West": 79,
}


# Date Ranges for Each Survey Wave
hfs_date_range = {
    1: ["2015-02-01", "2015-09-01"],
    2: ["2016-02-01", "2016-06-01"],
    3: ["2016-09-01", "2017-03-01"],
    4: ["2017-05-01", "2017-08-01"],
}

# Convert reported units to kg
wave4_units = {
    1: 10.0 / 1,  # placeholder; 10 liter
    2: 1000.0 / 100,
    3: 1000.0 / 200,
    4: 1 / 3.7,
    5: 1000.0 / 1,
    6: 1000.0 / 100,
    7: 1000.0 / 150,
    8: 1000.0 / 200,
    9: 1000.0 / 300,
    10: 1000.0 / 700,
    11: 1 / 12.0,
    12: 1.0,
    13: 1.0,  # placeholder; litre
    14: 1000.0 / 1,  # placeholder 'millilitre',
    15: 1.0,  # placeholder 'packet'
    16: 1000.0 / 200,
    17: 1000.0 / 20,
    18: 1000.0 / 30,
    19: 1000.0 / 400,
    20: 1000.0 / 50,
    21: 1000.0 / 70,
    22: 1000.0 / 2,
    23: 1000.0 / 40,
    24: 1000.0 / 50,
    25: 1000.0 / 100,
    26: 1000.0 / 200,
    27: 1000.0 / 250,
    28: 1000.0 / 300,
    29: 1000.0 / 350,
    30: 1000.0 / 500,
    31: 1000.0 / 900,
    32: 1.0,
    33: 1 / 1.25,
    34: 1 / 1.5,
    35: 1 / 10.0,
    36: 1 / 2.0,
    37: 1 / 3.0,
    38: 1.0,  # placeholder 'plate'
    39: 1 / 50.0,
    40: 1000.0 / 15,
    41: 1.0,  # placeholder 'one layer'
    42: 1000.0 / 300,
}


# Wave 4 doesn't include these translations
item_labels = {
    10101: "Dura",
    10102: "Yellow maize (Dura Shami)",
    10103: "Millet (Dukhn)",
    10104: "Wheat",
    10105: "Maize (in the cob)",
    10106: "Rice (imported)",
    10107: "Wheat flour (Fino,local)",
    10108: "Dura flour",
    10109: "Maize flour",
    10110: "Millet flour",
    10111: "Other flour",
    10112: "Macaroni, spaghetti, noodles etc",
    10113: "Breakfast cereals",
    10114: "Reels of pasta",
    10115: "Bread",
    10116: "Kisra and asida",
    10117: "Local biscuit",
    10118: "Buns",
    10119: "Infant feeding",
    10120: "Other cereals and cereal products (except infant feeding, wheat, millet, reels of pasta, buns and breakfast cereals)",
    10201: "Sheep meat (fresh, with bone, local)",
    10202: "Goat meat (with bones, fresh, local)",
    10203: "Liver (sheep/goat)",
    10204: "Feet/foot from sheep/goat (fresh and clean without skin)",
    10205: "Head from sheep/goat (fresh and clean without skin)",
    10206: "Mutton tripes/intestines but no liver from sheep/goat (fresh and clean without skin)",
    10207: "Fresh beef",
    10208: "Feet/foot from cow/veal (fresh and clean without skin)",
    10209: "Head from cow/veal (fresh and clean without skin)",
    10210: "Liver (cattle/veal)",
    10211: "Intestines beef/cow/veal but no liver",
    10212: "Pork meat",
    10213: "Camel liver",
    10214: "Chicken and poultry",
    10215: "Small animals (rabbits, mice,etc.)",
    10216: "Insects",
    10217: "Blood and blood products",
    10218: "Sausages (cattle/veal)",
    10219: "Other fresh meat and animal products but no sheep/goat or beef/cow/veal",
    10301: "Fresh fish, Bolati and others",
    10302: "Fissekh, salted fish (local)",
    10303: "Dried fish (local)",
    10304: "Tinned fish, sardine 125 grams, tuna, etc",
    10401: "Fresh milk",
    10402: "Milk powder",
    10403: "Milk products; cheese, yoghurt, etc",
    10404: "Eggs",
    10501: "Animal and vegetable butter",
    10502: "Ghee (samin)",
    10503: "Cooking oil",
    10601: "Apples",
    10602: "Local banana",
    10603: "Oranges",
    10604: "Mangoes",
    10605: "Indian mango (local)",
    10606: "Mango peal (municipal mango)",
    10607: "Pineapple",
    10608: "Dates",
    10609: "Papaya",
    10610: "Avocado",
    10701: "Dry Egyptian beans (local)",
    10702: "Dry chick peas",
    10703: "Green okra",
    10704: "Dry okra (dry Alweka)",
    10705: "Natural groundnut (Roasted)",
    10706: "Groundnut flour",
    10707: "Soya bean flour",
    10708: "Lentils",
    10709: "White beans",
    10710: "Lentils (Adasia)",
    10711: "Carrots",
    10712: "Cabbage",
    10713: "Cucumber",
    10714: "Onions",
    10715: "Fresh tomatoes",
    10716: "Potato (Irish)",
    10717: "Sweet potato",
    10718: "Milokhia",
    10719: "Pumpkin (Garaoa)",
    10720: "Tomato sauce (canned)",
    10721: "Tomato sauce (small pack of 70 grams)",
    10722: "Tomato sauce (large pack of local 500 grams)",
    10723: "Tinned pulses",
    10724: "Cassava tubers",
    10725: "Yam",
    10726: "Cassava flour",
    10727: "Cooking banana",
    10728: "Other roots, tubers, vegetables",
    10801: "Sugar",
    10802: "Sugar cane",
    10803: "Natural honey",
    10804: "Tahnieh Halawa",
    10805: "Chocolate",
    10806: "Jam (the malty) & jelly",
    10807: "Candy",
    10808: "Jelly",
    10901: "Green spicy (pungent)",
    10902: "Red chili (hot pepper)",
    10903: "Grain black pepper",
    10904: "Ginger powder",
    10905: "Yeast",
    10906: "Promises cinnamon",
    10907: "Cinnamon powder",
    10908: "Food salt",
    10909: "Baking powder",
    10910: "Coriander",
    10911: "Okra dry powder (waika)",
    11001: "Coffee",
    11002: "Black tea imported (no tea bags)",
    11003: "Khazalten tea or other",
    11004: "Tea bags",
    11005: "Nescafe (coffee instant)",
    11006: "Cocoa",
    11101: "Local mineral water",
    11102: "Local mineral water 1.5 liters",
    11103: "Local mineral water 0.5 liters",
    11104: "Orange juice (fruit juice)",
    11105: "Bottle of Fanta Sprite",
    11106: "Traditional beer",
    11107: "Canned/bottled beer",
    11108: "Liquor",
    11110: "Bottle of Fanta or Sprite 300-350 milliliters",
    11111: "Aluminium box Fanta or Sprite 350 milliliters",
    11201: "Cigarettes",
    11202: "Tombac, tobacco",
    11203: "Honeyed tobacco (Aoasl)",
    11301: "Lunch in a restaurant",
    11302: "Coffee or tea in the market",
    11303: "Fresh orange juice in a restaurant",
    11304: "Meals and breakfast for one person in a restaurant",
    11305: "Sandwich Tamiya / beans",
    11306: "Egyptian boiled beans",
    11401: "Maize boiled/roasted",
    11402: "Cassava boiled",
    11403: "Eggs boiled",
    11404: "Chicken",
    11405: "Meat",
    11406: "Fish",
    11407: "Meat dishes in a restaurant",
    11408: "Fish dishes in a restaurant",
}


# Wave 4 didn't have these yet
itemlabel_to_catlabel = {
    "Aluminium box Fanta or Sprite 350 milliliters": "Mineral water and refreshing drinks",
    "Animal and vegetable butter": "Oils and fats",
    "Apples": "Fruits",
    "Avocado": "Fruits",
    "Baking powder": "Other food products",
    "Black tea imported (no tea bags)": "Coffee, tea and cocoa",
    "Blood and blood products": "Meat",
    "Bottle of Fanta Sprite": "Mineral water and refreshing drinks",
    "Bottle of Fanta or Sprite 300-350 milliliters": "Mineral water and refreshing drinks",
    "Bread": "Bread and Cereals",
    "Breakfast cereals": "Bread and Cereals",
    "Buns": "Bread and Cereals",
    "Cabbage": "Pulses and vegetables",
    "Camel liver": "Meat",
    "Candy": "Sugar, jam, honey, chocolate and candy",
    "Canned/bottled beer": "Mineral water and refreshing drinks",
    "Carrots": "Pulses and vegetables",
    "Cassava boiled": "Cooked food from vendors",
    "Cassava flour": "Pulses and vegetables",
    "Cassava tubers": "Pulses and vegetables",
    "Chicken": "Cooked food from vendors",
    "Chicken and poultry": "Meat",
    "Chocolate": "Sugar, jam, honey, chocolate and candy",
    "Cigarettes": "Tobacco",
    "Cinnamon powder": "Other food products",
    "Cocoa": "Coffee, tea and cocoa",
    "Coffee": "Coffee, tea and cocoa",
    "Coffee or tea in the market": "Restaurants and cafes",
    "Cooking banana": "Pulses and vegetables",
    "Cooking oil": "Oils and fats",
    "Coriander": "Other food products",
    "Cucumber": "Pulses and vegetables",
    "Dates": "Fruits",
    "Dried fish (local)": "Fish and Seafood",
    "Dry Egyptian beans (local)": "Pulses and vegetables",
    "Dry chick peas": "Pulses and vegetables",
    "Dry okra (dry Alweka)": "Pulses and vegetables",
    "Dura": "Bread and Cereals",
    "Dura flour": "Bread and Cereals",
    "Eggs": "Milk, cheese and eggs",
    "Eggs boiled": "Cooked food from vendors",
    "Egyptian boiled beans": "Restaurants and cafes",
    "Feet/foot from cow/veal (fresh and clean without skin)": "Meat",
    "Feet/foot from sheep/goat (fresh and clean without skin)": "Meat",
    "Fish": "Cooked food from vendors",
    "Fish dishes in a restaurant": "Cooked food from vendors",
    "Fissekh, salted fish (local)": "Fish and Seafood",
    "Food salt": "Other food products",
    "Fresh beef": "Meat",
    "Fresh fish, Bolati and others": "Fish and Seafood",
    "Fresh milk": "Milk, cheese and eggs",
    "Fresh orange juice in a restaurant": "Restaurants and cafes",
    "Fresh tomatoes": "Pulses and vegetables",
    "Ghee (samin)": "Oils and fats",
    "Ginger powder": "Other food products",
    "Goat meat (with bones, fresh, local)": "Meat",
    "Grain black pepper": "Other food products",
    "Green okra": "Pulses and vegetables",
    "Green spicy (pungent)": "Other food products",
    "Groundnut flour": "Pulses and vegetables",
    "Head from cow/veal (fresh and clean without skin)": "Meat",
    "Head from sheep/goat (fresh and clean without skin)": "Meat",
    "Honeyed tobacco (Aoasl)": "Tobacco",
    "Indian mango (local)": "Fruits",
    "Infant feeding": "Bread and Cereals",
    "Insects": "Meat",
    "Intestines beef/cow/veal but no liver": "Meat",
    "Jam (the malty) & jelly": "Sugar, jam, honey, chocolate and candy",
    "Jelly": "Sugar, jam, honey, chocolate and candy",
    "Khazalten tea or other": "Coffee, tea and cocoa",
    "Kisra and asida": "Bread and Cereals",
    "Lentils": "Pulses and vegetables",
    "Lentils (Adasia)": "Pulses and vegetables",
    "Liquor": "Mineral water and refreshing drinks",
    "Liver (cattle/veal)": "Meat",
    "Liver (sheep/goat)": "Meat",
    "Local banana": "Fruits",
    "Local biscuit": "Bread and Cereals",
    "Local mineral water": "Mineral water and refreshing drinks",
    "Local mineral water 0.5 liters": "Mineral water and refreshing drinks",
    "Local mineral water 1.5 liters": "Mineral water and refreshing drinks",
    "Lunch in a restaurant": "Restaurants and cafes",
    "Macaroni, spaghetti, noodles etc": "Bread and Cereals",
    "Maize (in the cob)": "Bread and Cereals",
    "Maize boiled/roasted": "Cooked food from vendors",
    "Maize flour": "Bread and Cereals",
    "Mango peal (municipal mango)": "Fruits",
    "Mangoes": "Fruits",
    "Meals and breakfast for one person in a restaurant": "Restaurants and cafes",
    "Meat": "Cooked food from vendors",
    "Meat dishes in a restaurant": "Cooked food from vendors",
    "Milk powder": "Milk, cheese and eggs",
    "Milk products; cheese, yoghurt, etc": "Milk, cheese and eggs",
    "Millet (Dukhn)": "Bread and Cereals",
    "Millet flour": "Bread and Cereals",
    "Milokhia": "Pulses and vegetables",
    "Mutton tripes/intestines but no liver from sheep/goat (fresh and clean without skin)": "Meat",
    "Natural groundnut (Roasted)": "Pulses and vegetables",
    "Natural honey": "Sugar, jam, honey, chocolate and candy",
    "Nescafe (coffee instant)": "Coffee, tea and cocoa",
    "Okra dry powder (waika)": "Other food products",
    "Onions": "Pulses and vegetables",
    "Orange juice (fruit juice)": "Mineral water and refreshing drinks",
    "Oranges": "Fruits",
    "Other cereals and cereal products (except infant feeding, wheat, millet, reels of pasta, buns and breakfast cereals)": "Bread and Cereals",
    "Other flour": "Bread and Cereals",
    "Other fresh meat and animal products but no sheep/goat or beef/cow/veal": "Meat",
    "Other roots, tubers, vegetables": "Pulses and vegetables",
    "Papaya": "Fruits",
    "Pineapple": "Fruits",
    "Pork meat": "Meat",
    "Potato (Irish)": "Pulses and vegetables",
    "Promises cinnamon": "Other food products",
    "Pumpkin (Garaoa)": "Pulses and vegetables",
    "Red chili (hot pepper)": "Other food products",
    "Reels of pasta": "Bread and Cereals",
    "Rice (imported)": "Bread and Cereals",
    "Sandwich Tamiya / beans": "Restaurants and cafes",
    "Sausages (cattle/veal)": "Meat",
    "Sheep meat (fresh, with bone, local)": "Meat",
    "Small animals (rabbits, mice,etc.)": "Meat",
    "Soya bean flour": "Pulses and vegetables",
    "Sugar": "Sugar, jam, honey, chocolate and candy",
    "Sugar cane": "Sugar, jam, honey, chocolate and candy",
    "Sweet potato": "Pulses and vegetables",
    "Tahnieh Halawa": "Sugar, jam, honey, chocolate and candy",
    "Tea bags": "Coffee, tea and cocoa",
    "Tinned fish, sardine 125 grams, tuna, etc": "Fish and Seafood",
    "Tinned pulses": "Pulses and vegetables",
    "Tomato sauce (canned)": "Pulses and vegetables",
    "Tomato sauce (large pack of local 500 grams)": "Pulses and vegetables",
    "Tomato sauce (small pack of 70 grams)": "Pulses and vegetables",
    "Tombac, tobacco": "Tobacco",
    "Traditional beer": "Mineral water and refreshing drinks",
    "Wheat": "Bread and Cereals",
    "Wheat flour (Fino,local)": "Bread and Cereals",
    "White beans": "Pulses and vegetables",
    "Yam": "Pulses and vegetables",
    "Yeast": "Other food products",
    "Yellow maize (Dura Shami)": "Bread and Cereals",
}

# Food Groupings for AIDS model
food_groupings_old = {
    "Bread and Cereals": "Main_staples",
    "Coffee, tea and cocoa": np.nan,
    "Cooked food from vendors": np.nan,
    "Fish and Seafood": "Meat_and_fish",
    "Fruits": "Fruits_Vegetables",
    "Meat": "Meat_and_fish",
    "Milk, cheese and eggs": np.nan,
    "Mineral water and refreshing drinks": np.nan,
    "Oils and fats": np.nan,
    "Other food products": np.nan,
    "Pulses and vegetables": "Pulses",
    "Restaurants and cafes": np.nan,
    "Sugar, jam, honey, chocolate and candy": np.nan,
    "Tobacco": np.nan,
}

food_groupings = {
    "Bread and Cereals": "Bread and Cereals",
    "Coffee, tea and cocoa": np.nan,
    "Cooked food from vendors": np.nan,
    "Fish and Seafood": "Meat",
    "Fruits": np.nan,
    "Meat": "Meat",
    "Milk, cheese and eggs": "Milk, cheese and eggs",
    "Mineral water and refreshing drinks": np.nan,
    "Oils and fats": "Oils and fats",
    "Other food products": np.nan,
    "Pulses and vegetables": "Pulses and vegetables",
    "Restaurants and cafes": np.nan,
    "Sugar, jam, honey, chocolate and candy": "Sugar, jam, honey, chocolate and candy",
    "Tobacco": np.nan,
}


# The average calories (in cal) per gram for each food group
# @TODO: Come up with a better approximation.
food_groupings_calories = {
    "Bread and Cereals": 4,
    "Meat": 4,
    "Milk, cheese and eggs": 4,
    "Oils and fats": 9,
    "Pulses and vegetables": 4,
    "Sugar, jam, honey, chocolate and candy": 4,
}


adult_equivalence_conversions = {
    "Female": {
        0: 0.28,
        1: 0.4,
        2: 0.47,
        3: 0.53,
        5: 0.6,
        7: 0.62,
        10: 0.67,
        12: 0.72,
        14: 0.74,
        18: 0.72,
        30: 0.74,
        61: 0.67,
    },
    "Male": {
        0: 0.28,
        1: 0.4,
        2: 0.47,
        3: 0.53,
        5: 0.65,
        7: 0.72,
        10: 0.76,
        12: 0.83,
        14: 0.91,
        16: 0.98,
        18: 1.03,
        30: 1,
        61: 0.84,
    },
}

rename_food_columns = {
    1: {"cons_q_kg": "cons_quant_kg"},
    2: {"cons_q_kg": "cons_quant_kg"},
    3: {"E_15_pric_total": "price_ssp"},
    4: {
        "D_6_purc_unit": "purc_unit",
        "D_5_cons_unit": "cons_unit",
        "D_6_purc_quant": "purc_quant",
        "D_5_cons_quant": "cons_quant",
        "D_7_pric_total": "price_ssp",
    },
}

rename_hhm_columns = {
    1: {
        "BB_2_gender": "member_gender",
        "BB_4_age": "member_age_years",
        "BB_3_relation": "member_hoh_relation",
    },
    2: {
        "BB_2_gender": "member_gender",
        "BB_4_age": "member_age_years",
        "BB_3_relation": "member_hoh_relation",
    },
    3: {
        "B_15_gender": "member_gender",
        "B_2_age": "member_age_years",
        "B_16_relation": "member_hoh_relation",
    },
    4: {
        "B_7_hhm_gender": "member_gender",
        "B_1_hhm_age": "member_age_years",
        "B_8_hhm_relation": "member_hoh_relation",
    },
}


replace_hhm_values = {
    1: {"member_hoh_relation": {}, "member_gender": {}},
    2: {
        "member_hoh_relation": {
            2: "Wife or husband",
            3: "Son or Daughter (include adopted)",
            4: "Grandson or granddaughter",
            5: "Niece or Nephew",
            6: "Father or Mother",
            7: "Brother or sister",
            8: "Son or daughter in law",
            9: "Brother or sister in law",
            10: "Grandfather or grandmother",
            11: "Father or mother in law",
            12: "Other relative",
            13: "Servant or servants relative",
            14: "Lodger or lodger's relative",
            15: "Other non-relative",
            16: "Other",
            17: "Don't know",
            18: "Refused to respond",
            1000: ".Z",
        },
        "member_gender": {1: "Male", 2: "Female"},
    },
    3: {
        "member_hoh_relation": {
            2: "Wife or husband",
            3: "Son or Daughter (include adopted)",
            4: "Grandson or granddaughter",
            5: "Niece or Nephew",
            6: "Father or Mother",
            7: "Brother or sister",
            8: "Son or daughter in law",
            9: "Brother or sister in law",
            10: "Grandfather or grandmother",
            11: "Father or mother in law",
            12: "Other relative",
            13: "Servant or servants relative",
            14: "Lodger or lodger's relative",
            15: "Other non-relative",
            16: "Other",
            17: "Don't know",
            18: "Refused to respond",
            1000: ".Z",
        },
        "member_gender": {1: "Male", 2: "Female"},
    },
    4: {
        "member_hoh_relation": {
            2: "Wife or husband",
            3: "Son or Daughter (include adopted)",
            4: "Grandson or granddaughter",
            5: "Niece or Nephew",
            6: "Father or Mother",
            7: "Brother or sister",
            8: "Son or daughter in law",
            9: "Brother or sister in law",
            10: "Grandfather or grandmother",
            11: "Father or mother in law",
            12: "Other relative",
            13: "Servant or servants relative",
            14: "Other non-relative",
            -99: "Refused to respond",
            1000: ".Z",
        },
        "member_gender": {1: "Male", 2: "Female"},
    },
}

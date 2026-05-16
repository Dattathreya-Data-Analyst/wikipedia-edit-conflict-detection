"""Central configuration for the Wikipedia Edit Conflict Detection pipeline."""

import os

# -- Project paths --
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")

# -- Wikipedia data sources --
WIKI_REVISION_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
PAGEVIEW_API_TEMPLATE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/all-agents/{title}/daily/{start}/{end}"
)
WIKI_DUMPS_BASE = "https://dumps.wikimedia.org/enwiki/"
WIKIDATA_DUMPS_BASE = "https://dumps.wikimedia.org/wikidatawiki/entities/"

# -- Curated article sets by topic category --
POLITICAL_ARTICLES = [
    "Donald_Trump", "Climate_change", "Israel%E2%80%93Palestine_conflict",
    "Brexit", "Russian_invasion_of_Ukraine", "Joe_Biden",
    "Gun_control", "Abortion", "Black_Lives_Matter", "January_6_United_States_Capitol_attack",
    "2020_United_States_presidential_election", "Hillary_Clinton", "Immigration_to_the_United_States",
    "Russo-Ukrainian_War", "China%E2%80%93United_States_relations", "European_migrant_crisis",
    "Nuclear_weapons", "Vladimir_Putin", "COVID-19_pandemic_in_the_United_States",
    "North_Korea", "Xi_Jinping", "Crimea", "Gaza_Strip", "Iran",
    "Second_Amendment_to_the_United_States_Constitution", "Socialism", "Capitalism",
    "Communism", "Fascism", "Taliban", "War_in_Afghanistan_(2001%E2%80%932021)",
    "Syrian_civil_war", "Hong_Kong_protests", "Roe_v._Wade",
    "Gerrymandering", "Electoral_College_(United_States)", "NATO",
    "Barack_Obama", "Alexandria_Ocasio-Cortez", "Republican_Party_(United_States)",
    "Democratic_Party_(United_States)", "European_Union", "United_Nations",
    "Narendra_Modi", "Recep_Tayyip_Erdo%C4%9Fan", "Bolsonaro",
    "Benjamin_Netanyahu", "COVID-19_lab_leak_theory", "QAnon",
    "Proud_Boys", "Antifa_(United_States)", "Affirmative_action",
    "Death_penalty", "Same-sex_marriage", "Transgender_rights",
    "Police_brutality", "Islamophobia", "Antisemitism",
    "White_supremacy", "Critical_race_theory", "Woke",
    "Cancel_culture", "Freedom_of_speech", "Second_impeachment_of_Donald_Trump",
    "Mueller_investigation", "WikiLeaks", "Edward_Snowden",
    "NSA_warrantless_surveillance", "Patriot_Act", "Guantanamo_Bay",
    "Torture_Memos", "Abu_Ghraib_torture", "Drone_warfare",
    "Military%E2%80%93industrial_complex", "Lobbying_in_the_United_States",
    "Citizens_United_v._FEC", "Dark_money", "Voter_suppression",
    "Border_wall", "DACA", "Sanctuary_city",
    "Paris_Agreement", "Kyoto_Protocol", "Green_New_Deal",
    "Carbon_tax", "Fossil_fuel", "Oil_sands",
    "Fracking", "Pipeline_protest", "Standing_Rock",
    "Flint_water_crisis", "Environmental_racism", "Deforestation",
    "Amazon_rainforest", "Great_Barrier_Reef", "Extinction_Rebellion",
    "Greta_Thunberg", "Al_Gore", "Intergovernmental_Panel_on_Climate_Change",
    "Sea_level_rise", "Global_warming_controversy", "Climate_change_denial",
    "Renewable_energy", "Nuclear_power", "Solar_energy",
    "Wind_power", "Electric_vehicle", "Tesla_Inc.",
    "SpaceX", "Universal_basic_income", "Medicare_for_All",
    "Single-payer_healthcare", "Obamacare", "NHS",
    "Drug_policy", "War_on_drugs", "Legality_of_cannabis",
    "Opioid_epidemic", "Wealth_inequality", "Minimum_wage",
    "Trade_union", "Amazon_(company)", "Gig_economy",
    "Cryptocurrency", "Bitcoin", "Central_bank_digital_currency",
    "Federal_Reserve", "Inflation", "Great_Recession",
    "2008_financial_crisis", "Occupy_Wall_Street", "Panama_Papers",
    "Paradise_Papers", "Offshore_financial_centre", "Tax_haven",
    "Income_inequality_in_the_United_States", "Homelessness",
    "Student_debt", "Housing_bubble", "Gentrification",
    "Mass_incarceration", "Private_prison", "Death_row",
    "Wrongful_conviction", "Racial_profiling", "Stop-and-frisk",
    "War_on_terror", "ISIS", "Al-Qaeda",
    "September_11_attacks", "War_in_Iraq", "Libya_intervention",
    "Arab_Spring", "Yemen_civil_war", "Kashmir_conflict",
    "Rohingya_genocide", "Uyghur_genocide", "Tiananmen_Square_protests",
    "Falun_Gong", "Tibet", "South_China_Sea",
    "Taiwan", "Korean_conflict", "Cuban_Missile_Crisis",
    "Cold_War", "Berlin_Wall", "Soviet_Union",
    "Apartheid", "Nelson_Mandela", "Rwandan_genocide",
    "Darfur_genocide", "Armenian_genocide", "The_Holocaust",
    "Colonialism", "British_Empire", "Slavery_in_the_United_States",
]

SCIENTIFIC_ARTICLES = [
    "Evolution", "General_relativity", "DNA", "COVID-19", "Artificial_intelligence",
    "Quantum_mechanics", "Big_Bang", "Black_hole", "Dark_matter", "Dark_energy",
    "Higgs_boson", "String_theory", "Theory_of_everything", "Standard_Model",
    "Photosynthesis", "Mitochondrion", "Gene_editing", "CRISPR", "Stem_cell",
    "Vaccine", "mRNA_vaccine", "Antibiotic_resistance", "HIV/AIDS",
    "Cancer", "Alzheimer%27s_disease", "Depression_(mood)",
    "Schizophrenia", "Autism_spectrum", "ADHD",
    "Machine_learning", "Deep_learning", "Neural_network",
    "Natural_language_processing", "Computer_vision", "Robotics",
    "Nanotechnology", "Graphene", "Superconductivity",
    "Nuclear_fusion", "Tokamak", "ITER",
    "Gravitational_wave", "Exoplanet", "Mars_exploration",
    "James_Webb_Space_Telescope", "Hubble_Space_Telescope", "International_Space_Station",
    "Plate_tectonics", "Earthquake", "Volcano",
    "Tsunami", "Hurricane", "Tornado",
    "Paleontology", "Dinosaur", "Homo_sapiens",
    "Neanderthal", "Human_evolution", "Genetics",
    "Epigenetics", "Protein_folding", "AlphaFold",
    "Periodic_table", "Chemical_bond", "Organic_chemistry",
    "Thermodynamics", "Entropy", "Chaos_theory",
    "Fractals", "Game_theory", "Information_theory",
    "Cryptography", "Quantum_computing", "Turing_machine",
    "P_vs_NP", "Algorithm", "Data_structure",
    "Internet", "World_Wide_Web", "Blockchain",
    "Climate_science", "Oceanography", "Atmospheric_science",
    "Biodiversity", "Ecology", "Conservation_biology",
    "Endangered_species", "Coral_reef", "Rainforest",
    "Water_cycle", "Carbon_cycle", "Nitrogen_cycle",
    "Soil_science", "Agriculture", "Genetically_modified_organism",
    "Pesticide", "Organic_farming", "Food_security",
    "Nuclear_physics", "Particle_physics", "Astrophysics",
    "Cosmology", "Multiverse", "Anthropic_principle",
    "Fermi_paradox", "Drake_equation", "SETI",
    "Relativity", "Speed_of_light", "Spacetime",
    "Electromagnetism", "Maxwell%27s_equations", "Semiconductor",
    "Transistor", "Integrated_circuit", "Moore%27s_law",
    "Neuroscience", "Consciousness", "Free_will",
    "Psychology", "Cognitive_science", "Behaviorism",
    "Psychoanalysis", "Statistics", "Bayesian_inference",
    "Regression_analysis", "Probability_theory", "Calculus",
    "Linear_algebra", "Topology", "Number_theory",
]

CULTURAL_ARTICLES = [
    "The_Beatles", "Star_Wars", "Olympic_Games", "FIFA_World_Cup",
    "Shakespeare", "Leonardo_da_Vinci", "Mona_Lisa", "Beethoven",
    "Mozart", "Pablo_Picasso", "Vincent_van_Gogh", "Frida_Kahlo",
    "Harry_Potter", "The_Lord_of_the_Rings", "Marvel_Cinematic_Universe",
    "Disney", "Pixar", "Studio_Ghibli", "Anime", "Manga",
    "Hip_hop", "Jazz", "Rock_and_roll", "Classical_music", "K-pop",
    "Bollywood", "Hollywood", "Netflix", "YouTube", "TikTok",
    "Instagram", "Facebook", "Twitter", "Reddit", "Wikipedia",
    "Google", "Apple_Inc.", "Microsoft", "Video_game", "Esports",
    "Chess", "Go_(game)", "Poker", "Basketball", "Football",
    "Cricket", "Tennis", "Formula_One", "Tour_de_France", "Super_Bowl",
    "Academy_Awards", "Grammy_Awards", "Cannes_Film_Festival", "Nobel_Prize",
    "Pulitzer_Prize", "Michelin_Guide", "Fashion", "Haute_couture",
    "Chanel", "Louis_Vuitton", "Nike_Inc.", "Coca-Cola", "McDonald%27s",
    "Coffee", "Wine", "Beer", "Whisky", "Sushi",
    "Pizza", "Chocolate", "Tea", "Yoga", "Meditation",
    "Buddhism", "Christianity", "Islam", "Hinduism", "Judaism",
    "Atheism", "Mythology", "Greek_mythology", "Norse_mythology",
    "Ancient_Egypt", "Roman_Empire", "Ancient_Greece", "Renaissance",
    "Enlightenment", "Industrial_Revolution", "World_War_I", "World_War_II",
    "Space_Race", "Moon_landing", "Woodstock", "Burning_Man",
    "Carnival", "Oktoberfest", "Chinese_New_Year", "Diwali",
    "Christmas", "Halloween", "Thanksgiving", "Valentine%27s_Day",
    "Tourism", "Eiffel_Tower", "Great_Wall_of_China", "Machu_Picchu",
    "Taj_Mahal", "Colosseum", "Stonehenge", "Pyramids_of_Giza",
    "New_York_City", "London", "Tokyo", "Paris", "Rome",
    "Sydney", "Rio_de_Janeiro", "Dubai", "Singapore", "Hong_Kong",
    "Photography", "Cinema", "Television", "Radio", "Newspaper",
    "Publishing", "Comic_book", "Graphic_novel", "Science_fiction",
    "Fantasy", "Horror_fiction", "Mystery_fiction", "Romance_novel",
    "Poetry", "Theatre", "Opera", "Ballet", "Dance",
    "Architecture", "Sculpture", "Painting", "Street_art", "Graffiti",
    "Tattooing", "Cuisine", "Gastronomy", "Veganism", "Vegetarianism",
]

# Complete curated article set across all categories
ALL_ARTICLES = POLITICAL_ARTICLES + SCIENTIFIC_ARTICLES + CULTURAL_ARTICLES

# -- Pageview date range --
PAGEVIEW_START = "20200101"
PAGEVIEW_END = "20241231"

# -- HDFS paths --
HDFS_URL = "http://namenode:9870"
HDFS_REVISIONS_DIR = "/user/dattatheya/wikipedia/revisions"
HDFS_WIKIDATA_DIR = "/user/dattatheya/wikidata/entities"
HDFS_PAGEVIEWS_DIR = "/user/dattatheya/pageviews/daily"

# -- MongoDB --
MONGO_HOST = "mongodb"
MONGO_PORT = 27017
MONGO_USERNAME = "root"
MONGO_PASSWORD = "root"
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
MONGO_DB = "diss"
MONGO_COLLECTION_EDIT_METRICS = "article_edit_metrics"
MONGO_COLLECTION_NETWORK = "editor_network_metrics"
MONGO_COLLECTION_PREDICTIONS = "stability_predictions"
MONGO_COLLECTION_TOPIC_COMPARISON = "topic_comparison"

# -- Spark --
SPARK_MASTER = "local[*]"
SPARK_APP_NAME = "WikipediaEditConflictDetection"

# -- Parquet output --
PARQUET_REVISIONS = os.path.join(DATA_DIR, "revisions_clean.parquet")
PARQUET_WIKIDATA = os.path.join(DATA_DIR, "wikidata_clean.parquet")
PARQUET_PAGEVIEWS = os.path.join(DATA_DIR, "pageviews_clean.parquet")
PARQUET_JOINED = os.path.join(DATA_DIR, "article_features.parquet")

# -- ML parameters --
REVERSION_THRESHOLD = 0.15  # articles with reversion rate above this are "unstable"
TEST_SPLIT = 0.2
RANDOM_SEED = 42

# -- MediaWiki API parameters --
REVISION_BATCH_SIZE = 50  # revisions to fetch per API call
API_RATE_LIMIT_DELAY = 0.5  # seconds between API requests to respect rate limits
MAX_REVISIONS_PER_ARTICLE = 5000  # cap to keep dataset manageable

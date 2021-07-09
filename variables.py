HOME_URL = 'https://www.homedepot.com/'
HOME_URL1 = 'https://www.homedepot.com'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36',
           'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
PRODUCTS_PER_PAGE = 24

required_data = {
        'navigation': [
            {
                'department': 'Appliances',
                'category_name': 'Dishwashers',
                'sub_category_name': '',
                'brands': ['LG', 'Samsung'],
                'store_ids': [6177, 589],
                'delivery_zip': [10022, 75209]
            },
            {
                'department': 'Appliances',
                'category_name': 'Refrigerators',
                'sub_category_name': '',
                'brands': ['Whirlpool', 'GE Appliances'],
                'store_ids': [6177, 589],
                'delivery_zip': [10022, 75209]
            },
            {
                'department': 'Decor & Furniture',
                'category_name': 'Bedroom Furniture',
                'sub_category_name': 'Mattresses',
                'brands': ['Sealy'],
                'store_ids': [6177, 589],
                'delivery_zip': [10022, 75209]
            }
            ]
        }


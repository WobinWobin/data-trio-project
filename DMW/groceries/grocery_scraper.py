import os
import re
import time
import threading
import requests
import pandas as pd
from datetime import date
from tqdm import tqdm
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =============================================================================
# CONFIG
# =============================================================================

STORE_NAME       = "SM Markets"
SITEMAP_PRODUCTS = "https://smmarkets.ph/media/sitemap-1-2.xml"
OUTPUT_PATH      = "results/grocery_data.csv"
DRIVER_PATH      = r"C:\Users\trist\Downloads\DMW\data-trio-project\DMW\groceries\msedgedriver.exe"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

PAGE_LOAD_WAIT = 8   # max seconds to wait for page before giving up
POLITE_DELAY   = 1.0 # seconds between requests per browser (ethical minimum)
NUM_BROWSERS   = 5   # number of parallel Edge instances — adjust based on your RAM
                     # each browser uses ~150-200MB. 5 browsers ≈ 1GB RAM
MAX_WORKERS    = 8

# Ordered from most specific to least to avoid wrong matches
NAME_RULES = [
    # Fresh Vegetables — covers vendor-coded items (BCA, BPS, NVR, SM Bonus, etc.)
    (r'\b(sayote|upo|patola|singkamas|alugbati|saluyot|kamote leaves|kamote|ampalaya|sitaw|stringbeans|okra|radish|cucumber|carrots?|potatoes?|tomatoes?|leeks?|wansoy|parsley|kinchay|batuan|tanglad|basil|chopseuy|pechay|togue|tokwa|tofu|mushroom|shimeji|broccoli|cauliflower|cabbage|lettuce|spinach|celery|eggplant|corn|mongo|beans|peas|malunggay|bokchoy|bok choy|sungsong|spring onion|onionleeks|bell pepper|jalapeno|leek|asparagus|turmeric|chili fingers|chopseuy mix|alugbati|saluyot|singkamas|patola)\b', "Fresh - Vegetables"),
    (r'\b(mango|banana|apple|grapes?|melon|pineapple|orange|papaya|avocado|lemon|lime|strawberr|watermelon|guava|coconut|calamansi|pomelo|kiwi|lanzones|rambutan|durian|jackfruit|langka|santol|siniguelas|ponkan|guyabano|atis|duhat|balimbing|mangosteen|honeydew|mandarin|navel orange|seedless|red globe|fuji|gala|washington|dragon fruit|lychee|pear|peach|plum|cherry|blueberr|cranberr|raspberry|saba|lakatan|lacatan|guapple)\b', "Fresh - Fruits"),
    (r'\b(beef|baka|bulalo|ribeye|rib.?eye|sirloin|tenderloin|wagyu|t.?bone|tbone|kalitiran|kenchie|maskara|porterhouse|prime rib|top round|round steak|breakfast steak|korean bbq|chuck rib|ground round|ox tail|ox feet|bone marrow|camto|shin|osso buco)\b', "Fresh - Beef"),
    (r'\b(pork|baboy|liempo|kasim|pigue|belly|chop|lechon|tocino|longganisa|chorizo|pata|knee cap|ox tripe|lumpiang shanghai|shanghai mix|skinless longanisa|sinigang cut|american ribs|leg quarter|pigs feet|pigs head)\b', "Fresh - Pork"),
    (r'\b(chicken|manok|thigh|breast|drumstick|wings?|whole bird|tinola cut|torikatsu|torikaraage)\b', "Fresh - Chicken"),
    (r'\b(fish|isda|tilapia|bangus|salmon|tuna|shrimp|hipon|squid|pusit|crab|alimango|oyster|tahong|clam|prawn|lapu.?lapu|maya.?maya|bisugo|danggit|dilis|espada|salinas|sapsap|bolinao|salay|lagao|tuyo|hibe|katambak|kasig|tabagak|suahe|pasayan|galunggong|alumahan|marlin|hasa|bolao|bilong|tanguigue|mat.?an|pesogo|crabmeat|crabstick|cuttlefish|fishcake|kingcrab|snowcrab|cod|halibut|bacalao|barramundi|dalagang|kapak|lapad|nokus|dulong|pampano|saranggani seabass|seabass|mussels|scallop)\b', "Fresh - Seafood"),
    # Frozen
    (r'\b(ice.?cream|gelato|sherbet|popsicle|ice bar|ice dessert|mochi ice|futaba|yawamochi|carmen.?s best|selecta|creamy delight|binggrae|pangtoa|samanco|melona)\b', "Frozen - Ice Cream & Desserts"),
    (r'\bfrozen\b.*\b(beef|baka)\b', "Frozen - Beef"),
    (r'\bfrozen\b.*\b(pork|baboy|liempo)\b', "Frozen - Pork"),
    (r'\bfrozen\b.*\b(chicken|manok)\b', "Frozen - Chicken"),
    (r'\bfrozen\b.*\b(fish|seafood|shrimp|squid)\b', "Frozen - Seafood"),
    (r'\bfrozen\b.*\b(vegetable|veg)\b', "Frozen - Vegetables"),
    (r'\b(french fries|crinkle cut|wedge cut|shoestring|hash brown|gyoza|dumpling|mandu|mantou|siomai|xiao long bao|shabu shabu|tteokbokki|bibimbap|fried rice|yakisoba|takoyaki|mochi waffle|corndog|spring roll|allgroo|hana mandoo|k chef|wang.*dumpling|baijia|niko.*gyoza|df sharksfin|allgroo kimchi|allgroo vegetable|dorayaki|bambi dumpling|bambi tortilla|bambi pizza|fat.*thin siao longpao|fat.*thin cua pao|hotsa)\b', "Frozen - Ready to Heat"),
    (r'\bfrozen\b', "Frozen"),
    # Bakery
    (r'\b(cake|pastry|muffin|croissant|donut|doughnut|ensaymada|pianono|piaya|polvoron|otap|barquillos|hojaldres|rosquillos|biscocho|broas|mamon|cupcake|brownie|waffle|cream puff|cream roll|tart|chiffon|tiramisu bar|mooncake|tikoy|hopia|pastillas|yema|ube ball|ube bites|taisan|bijon|dinner square|mochi roll|marby cakes|de ocampo|eng seng tower|vjandep|navarro.*pastillas|natys best|tian seng|polland ube|merci piaya|merci broas|tiffany|gardenia cookies|goldilocks cookies|fujiya milky|monteur|hokkaido cream)\b', "Bakery - Cakes & Pastries"),
    (r'\b(bread|loaf|pandesal|tasty|wheat bread|bun|bagel|baguette|brioche|sourdough|focaccia|pita|tortilla|wrap|roti|monay|kababayan|burger bun|hotdog bun|dinner roll|mini roll|marby buns|village gourmet|gardenia white|uncle george|st ives|turks pita|marby pita|marby ubod)\b', "Bakery - Bread"),
    # Dairy
    (r'\b(butter|margarine|buttercup|dari creme|anchor butter|president unsalted|magnolia buttercup)\b', "Dairy - Butter & Margarine"),
    (r'\b(cheese|quezo|queso|kesong|cheddar|gouda|havarti|parmesan|mozzarella|emmental|brie|feta|quickmelt|quick melt|cheezee|magnolia cheezee|eden cheddar|arla|bega|california cheddar|emborg|sterilgrda|magnolia quickmelt|eden singles|happy valley shredded)\b', "Dairy - Cheese"),
    (r'\b(yogurt|yakult|cultured milk|chobani|dairy farmers|pascual yogtetra|creamy delight greek|milkana|dutchmill)\b', "Dairy - Yogurt"),
    (r'\b(\begg\b|itlog|salted egg|brown egg|fresh egg|cage free|selenium egg|vitamin d egg|bounty egg|elrich egg|alto palo)\b', "Dairy - Eggs"),
    (r'\b(powdered milk|milk powder|growing.?up milk|bear brand|birch tree|anchor milk grain|anlene|sustagen|pediasure|ensure|nestogen|bonakid|ascenda|lactum|enfagrow|nido|s26|promil|nan\b|similac|enfamil|follow.?on|infant formula|fortified powder|adult boost)\b', "Dairy - Powdered Milk"),
    (r'\b(fresh milk|full cream|low fat milk|skim milk|uht milk|evaporated|condensada|condensed|all purpose cream|nestle cream|magnolia apc|cooking cream|whipping cream|sour cream|crema|kremdensada|hopla|everchef|emborg sour|bulla|president cooking|president whipping|president skimmed|selecta farm fresh|selecta filled|selecta non fat|jersey|milk magic|vitasoy|vitamilk|nutriboost|chuckie|alaska evaporada|angel all purpose|angel kremdensada|liberty condensada|cream all creamer|nestle coffeemate)\b', "Dairy - Milk & Cream"),
    # Pantry
    (r'\b(cooking oil|vegetable oil|olive oil|canola|palm oil|coconut oil|minola|dona elena|goodlife sesame|sesame oil|mua yu|truffle oil|kadoya|sunbest hon mirin|mirin|ryoroushu|sake|hinode|takara|cooking sake|molinera white truffle)\b', "Pantry - Oil"),
    (r'\b(tortellini|gnocchi|rigatoni|linguine|orzo|farfalle|tagliatelle|lasagna|pasta|spaghetti|penne|fettuccine|macaroni|vermicelli|miki|hofan|lungkou|somen|couscous|ideal gourmet tagliatelle|gallo|barilla|san remo|sunshine saucy ghetti|sunshine saucey ghetti|mega prime vermicelli|pearl tower longkou|jumy lungkou|tiffany bijon)\b', "Pantry - Pasta & Noodles"),
    (r'\b(ramyun|ramen|shin ramyun|nongshim|nissin cup|cup noodle|bowl noodle|samyang|yakisoba|gaga goreng|happy mie|mi goreng|koka instant|mi sedaap|payless pansit|udon|sotanghon|bihon|misua|imee vegetable noodles|nutrifam shirataki|six fortune|sunlee yellow|toasted miki|goodluck toasted|maruchan|tablemark)\b', "Pantry - Pasta & Noodles"),
    (r'\b(sardine|mackerel|tuna can|corned beef|luncheon meat|spam|555\b|hakone|king cup sardines|ocean flavor|unipak|mega mackerel|master mackerel|master sardines|reno potted|pearl river|carne norte|star carne|karne norte|giniling|argentina giniling|jolly fresh pitted|jolly peach|del monte sliced peaches|falcons valley peach|falcons valley whole kernel|today.?s mixed fruit|sunbest longan|sunbest blueberries|seasons fruit mix|ceres|phil supreme jack fruit|phil supreme macapuno|green harvest kaong|green harvest nata|kayumanggi|phil brand nata)\b', "Pantry - Canned Goods"),
    (r'\b(jam|jelly|peanut butter|mayo|mayonnaise|ketchup|dressing|ranch|best food|lady.?s choice|sandwich spread|magnolia sandwich|eden sandwich|thousand island|st dalfour|fruit spread|ovomaltine spread|nutella|cheez whiz)\b', "Pantry - Spreads & Dressing"),
    (r'\b(soup|broth|porridge|congee|lugaw|arroz caldo|miso|camp bells|cream of mushroom|massel|vegetable stock|laksa paste|pho|ultra cubes|por kwan laksa|hai.?s instant)\b', "Pantry - Soups & Porridge"),
    (r'\b(oat|cereal|granola|muesli|cornflakes|kelloggs|quaker|australia harvest|carmans|sante whole grain)\b', "Pantry - Oats & Cereals"),
    (r'\b(stevia|equal|splenda|honey|muscovado|brown sugar|white sugar|sweetener|syrup|mana sugar|alter trade|island coco sato|mascobado|iodized salt|rock salt|sunbeam|sm bonus refined sugar|sea salt|white diamond sea salt|coppola sale marino|himalayan.*salt|pink crystal himalayan|sa himalayan)\b', "Pantry - Honey & Sweeteners"),
    (r'\b(magic sarap|knorr|ajinomoto|seasoning|marinade|gravy|broth cube|carbonara.*mix|pasta sauce mix|white king|baking mix|cake mix|pancake mix|flour mix|mccormick|lasap mix|s&b golden curry|real thai|ahg.*curry|3 chefs.*curry|way sauce|kasugai tempura|tempura batter|caramba|american garden|franks red hot|french mustard|heinz.*mustard|kraft.*mustard|morehouse mustard|frenchs|worcestershire|sweet baby rays|ragu|sig sel barbecue|kraft barbecue|o.food|cj hechandle|cj gochujang|ebara sauce|sig sel sloppy|sig sel.*island|maruha.*yakisoba)\b', "Pantry - Condiments & Sauces"),
    (r'\b(soy sauce|fish sauce|oyster sauce|toyo|patis|bagoong|alamang|hot sauce|lechon sauce|bbq sauce|tomato sauce|tomato paste|chili oil|chili sauce|pesto|sinamak|sukang puti|sukang pula|vinegar|camel sukang|rikoys|marca pina|silver swan|datu puti|ufc cheesy|ufc meaty|ufc creamy|ufc oppa|ufc shake|ufc sweet pickle|ufc fun chow|ufc kusina|seaglow|figaro olives|capri olives|fragata|molinera.*olives|molinera.*jalapenos|molinera.*truffle|ram relish|ram pickles|sunbest bamboo|michigan made|phil supreme halayang|phil supreme garbanzos|kayumanggi garbanzos|green harvest pickle|sig sel artichoke|sig sel sauerkraut|mothers best spiced|nakedly|bicolsbest)\b', "Pantry - Condiments & Sauces"),
    (r'\b(rice|bigas|sinandomeng|jasmine rice|dinorado|adlai|quinoa|chia|flaxseed|buckwheat|millet|sorghum|golden phoenix|harvesters|rap and rai malagkit|rap and rai monggo|fc hi fiber|grain fusion|long grain|short grain|harvester.?s)\b', "Pantry - Rice & Grains"),
    (r'\b(flour|cornstarch|gawgaw|liwayway|la estrella|sm bonus all purpose flour|baking soda|baking powder|cream of tartar|gelatin|knox gelatin|ferna gelatine|yeast|breading mix|menu breading|amoren crispy coating|ellie all purpose)\b', "Pantry - Baking"),
    (r'\b(spice|paprika|cinnamon|curry powder|turmeric powder|cayenne|chili powder|cumin|cardamom|coriander|bayleaf|star anise|sesame seed|ispice|badia|mccormick cinnamon|arbis spanish paprika|shien shien paprika|shakti baba|green forest curry|dj spice|sfuw|sanwa wasabi|yamasa tempura|fat.?thin star anise|fat.?and.?thin star anise)\b', "Pantry - Spices & Seasonings"),
    # Snacks
    (r'\b(nori|seaweed|wakame|mizuho norimaki|mizuho kakinotane|regent crisps seaweed)\b', "Snacks - Seaweed"),
    (r'\b(marshmallow|nougat|markenburg|marken longlegs|choko choko|sweet dart|ring pop|lala ube pastillas|navarro pastimallows|eng seng tower yema|vjandep pastel|frooty milky pop)\b', "Snacks - Candies"),
    (r'\b(candy|gummy|trolli|chewing gum|menthol gum|columbias icool|mentos|halls|tic.?tac|hi.?chew|skittles|warheads|haribo|cokoc|halloween bears|lush sour|sweet station|gingerbon|fisherman.?s friend|ricola|himalaya berry|himalaya pepermint|himalaya vajomba|chupa chups|lipps pop stix|babble joe tutti|ya yammy tamarind|ya yammy sampalok|strikinng popping|columbia.?s yakee|juju secret santa)\b', "Snacks - Candies"),
    (r'\b(biscuit|cracker|cookie|wafer|oreo|skyflakes|fita|rebisco|mcvitie|digestive|hwa tai|haitai|kokola malkist|malkist|juju milk cream|danish milk sandwich|danish strawberry|julie.?s|monde|suncrest|leslie|snacku vegetable crackers|croley|jeanne.*jamie|shapes danish|shapes ugoy|rimi danish|cowhead|carmans cookies|carmans greek|lotus biscoff|arnotts timtam|pocky|glico|pringles|ruffles|chips delight|brownie break|m.y. san graham|noceda jacobina|nacho chips|regent big mouth|leslie cheezy|leslie clover|leslie nacho|jack.*jill mr chips|jack.*jill bag o fun|jack.*jill wafretz|jack.*jill lush|suncrest crossini|suncrest topps|quaker cookies|carmans super berry)\b', "Snacks - Biscuits & Crackers"),
    (r'\b(chip|crisp|piattos|nova|tortilla chips|popcorn|chicharon|kropek|lays|doritos|cheetos|oishi|ruffles|bawang na bawang|california cassava|bahaghari taro chips|maxi tropical roots|cravewell purple roots|ya yammy chichacorn|expo coated peanuts|expo greaseless|chickboy popnik|goya take it|maxi tropical|frabelle.*dilis|frabelle.*sweet.*spicy)\b', "Snacks - Chips & Dips"),
    (r'\b(nut|peanut|cashew|almond|pistachio|walnut|trail mix|macadamia|pili nuts|sunflower seeds|cha cheer|smart choice.*almond|smart choice.*cashew|smart choice.*pistachio|smart choice.*walnut|smart choice.*pumpkin|smart choice.*sunflower|blue diamond|heritage almonds|heritage walnuts|sunkist pistachios|katliens mixed nuts|jovy crispy pili|rpm glazed pili|rpm roasted pili|dailyfix|daily fix berry|coco everyday mix nuts|coco sunflower|smart choice dried cranberries|boy bawang|jbc happy peanuts)\b', "Snacks - Nuts & Seeds"),
    (r'\b(chocolate|choco|kitkat|snickers|twix|reese|toblerone|ferrero|chocotube|hershey|cadbury|lindt|belgian|van houten|violet crumble|meiji kouka|goya very berries|goya take it.*matcha|arcor bon o bon|kinder|dutche|rimi gifts chocolates|culture blends|sheila g|cloud 9|picc adelli|murgerbon|coco macadamia|coco kokokiss|coco cream bar|coco nougat|garuda gery chocolatos|columbia frooty|hueza toffee|orion milk cream|sabroso cacao|sabroso tsokolate|konu cone bites|konu mini krunch|glico pocky|glico cookies|glico strawberry tin|meiji lucky stick|meiji hello panda|hello panda|fujiya milky|monteur belgian|swiss miss|goya take it 4 fingers)\b', "Snacks - Chocolates"),
    (r'\b(tamarind|sampalok|dried mangoes|cebu dried mangoes|phil brand dried|hueza lengua|minco mangosteen|tots barquiron|tots pinasugbo|sugar kiat|fry.*pop lobster|bpop|sweet station|cravewell straw|cravewell papple|nacho chips colored|nacho chips plain|nacho chips sour|nacho chips barbeque|nni nni strawberry|expo coated|expo greaseless)\b', "Snacks - Others"),
    # Beverages
    (r'\b(coffee|nescafe|kopiko|barako|espresso|3.?in.?1|cafe\b|jardin|bottled coffee|iced coffee|starbucks|ucc coffeeblend|ucc roastmaster|great taste|lo.?r essenso|owl kopitiam|mountain brew|chingu cafe|cantata|nestle house blend|goya matcha latte|brewbrite)\b', "Beverage - Coffee & Tea"),
    (r'\btea\b|lipton|nestea|green tea|iced tea|herbal tea|twinings|heath.*heather|korean one ginseng', "Beverage - Coffee & Tea"),
    (r'\b(cocoa|milo|ovaltine|choco malt|chuckie|nutriboost)\b', "Beverage - Cocoa & Malt"),
    (r'\b(juice|four seasons|minute maid|c2|powerade|gatorade|flavored drink|tampico|ceres|philbrand|del monte tipco|gaya farm|blue cactus|extra joss|smart c|vida c|rite n lite|pocari sweat|monster energy|teazle zero|paldo pororo)\b', "Beverage - Juice & Drinks"),
    (r'\b(softdrink|soda|cola|pepsi|coke|royal|sprite|mountain dew|7.?up|mirinda|a&w rootbeer|lotte milkis)\b', "Beverage - Softdrinks"),
    (r'\b(mineral water|distilled water|spring water|wilkins|absolute|viva water|summit natural|perrier|sip plus himalayan)\b', "Beverage - Water"),
    (r'\b(beer|red horse|san miguel|pale pilsen|heineken|corona|shandy|san mig light)\b', "Beverage - Beer"),
    (r'\b(wine|martini|rose wine|moscato|merlot|cabernet|lambrusco|chamdor|barefoot|riunite|hardys|yellow tail|woomera|carlos light|tuka|fundador|gsm blue|tanduay|rum\b|gin\b|vodka|tequila|liquor|brandy|whisky|whiskey|scotch|bourbon|moutai|dalmore|singleton|macallan|johnnie walker|alfonso|sake\b|hana akita|kome ichizu|ube cream liqueur|walsh curacao|charles.*james)\b', "Beverage - Alcoholic"),
    # Health & Medicine
    (r'\b(capsule|tablet|syrup|supplement|vitamin|mineral|atc\b|pharex|ceelin|centrum kid|enervon|ferrous sulfate|solmux|decolgen|alaxan|flanax|vicks|katinko|salonpas|tiger balm|ammeltz|rhea cold rub|efficascent|nin jiom|strepsils|bye bye fever|tempra cool|mediplast|polident|dental b tbrush|sansflou|sansfluo|yamang bukid turmeric)\b', "Health & Medicine"),
    (r'\b(ethyl alcohol|isopropyl alcohol|biogenic|greencross|safe more isoprophyl|guardian isoprophyl|dr j ethyl|dr j iso|alcoplus|cleene hand sanitizer|bench alcogel|naturelab.*alco|naturelab.*alcohol)\b', "Health & Medicine"),
    # Babies & Kids
    (r'\b(diaper|pampers|huggies|drypers|lampein pants|mamy poko|eq pants)\b', "Babies & Kids - Diapers"),
    (r'\b(nan\b|similac|enfamil|s-26|s26\b|promil|nido\b|follow.?on|infant formula|bonakid|ascenda|lactum|enfagrow|nestogen|pediasure|ensure gold)\b', "Babies & Kids - Formula Milk"),
    (r'\b(baby food|gerber|cerelac|nestum|heinz.*custard|heinz.*vanilla|aveeno baby|johnson baby|babyflo bath|babyflo powder|babyflo cologne|babyflo baby|tiny buds|smart steps baby|lactacyd baby)\b', "Babies & Kids - Baby Food & Care"),
    # Wipes (broad — babies, personal, household)
    (r'\b(baby wipes|cherub baby wipes|kleenex pure water wipes|babyflo cotton buds|tender love wipes|nurture wipes|nursy wipes|uni love wipes|unilove.*wipes|punaas wipes|best lab wipes|bestlab wipes|sanicare wipes|cleene.*wipes|cuddles cotton buds|care underpads|megan cotton pads|bestlab cotton pads|bouncy.*tissue|cleene cotton pad)\b', "Home Care - Paper & Disposables"),
    # Home Care
    (r'\b(ariel|tide\b|surf\b|downy|fabric conditioner|zonrox|bleach|detergent|laundry|breeze capsules|breeze powder|speed det|calla det|speed bar|kalinisan fabcon|snuggle fabcon|uni love fabric|unilove peppa)\b', "Home Care - Laundry"),
    (r'\b(lysol|ajax|domex|mr\.? clean|pinesol|toilet bowl|floor wax|cleaner|krest disinfectant|family guard disinfectant|mighty mom dishwashing|joy dishwashing|arm.*hammer|mr muscle|febreze|febreeze|natucair|farcent|ambi pur|glade|koala tablet deodorizer|easy scoop odor|vet core odor|porma shoe)\b', "Home Care - Cleaning & Air Care"),
    (r'\b(baygon|raid|insect|mosquito|cockroach|pest|hoyhoy trap|advanced tracking powder|bayopet flea|shield gard flea|doggies choice tick|petgard dog powder)\b', "Home Care - Pest Control"),
    (r'\b(tissue|paper towel|napkin|trash bag|garbage bag|sando bag|cling wrap|aluminum foil|cotton roll|cotton buds|cotton pads|ziploc|freezer bag|disposable cups|straw\b|wooden chopstick|wooden stirrer|paper cup|paper meal box|hamburger box|sauce cup|bento box|lil princess|little princess|carnival.*cups|glow.*bag|glow.*tray|glow.*zip bag|glow.*paper cups|glow.*toothpick|glow ice bag|rollo bio|calypso options|basong pinoy|happy bee aluminum|cheers aluminum|cheers baking paper|panbake|starchware|eco innovators|uncle johns|skz bento|bouncy vp|bouncy kitchen towel|happy cotton rolls|wipe absorbent)\b', "Home Care - Paper & Disposables"),
    (r'\b(candle|esperma|manila wax|liwanag)\b', "Home Care - Others"),
    # Personal Care
    (r'\b(shampoo|pantene|sunsilk|rejoice|head.?shoulders|dandruff|creamsilk|being conditioner|mane n tail|tresemme|kerasys|tsubaki|keratin plus|vitakeratin|pregroe|symply g.*cond|symply g.*treatment|l.?oreal ever pure|black beauty conditioner|hana conditioner|cream silk hair|ellips hair)\b', "Personal Care - Hair"),
    (r'\b(hair treatment|hair dye|hair color|hair colorant|garnier hair|loreal exclusive|icolor|megan hair color|kolours hair dye|vitress|grips hair|gatsby.*wax|gatsby.*pomade|gatsby.*gel|gatsby.*spray|bench fix|hair clay|hair gel|hair wax|hair pomade|hair spray)\b', "Personal Care - Hair"),
    (r'\b(deodorant|roll.?on|antiperspirant|body spray|body wash|shower gel|safeguard|palmolive|dove\b|nivea|vaseline|lotion|prickly heat|fissan|talcum powder|enchanteur powder|deoplus|old spice|axe\b|gatsby body spray|gatsby cologne|bench body spray|bench daily|blackwater deo|bw women deo|penshoppe deo|penshoppe body|secret deo|gillette deo|brut deo|cathydoll deo|active white salt scrub|active white moisturizer|hello glow lot|block.*glow lot|ar body cream|ar moisturizing|ar whitening|gt moisturizing|gt whitening|kawaii max gluta|kojie san sun protect|gluta body soap|chupa chups bodywash|aveeno bodywash|hello glow sunflower|hello glow 2in1|hello glow hair removal|lucas papaw|mei yi massage)\b', "Personal Care - Body"),
    (r'\b(soap\b|bioderm|hygienix|jergens|shield soap|perla\b|activex|tokyo white body soap|seaoul white body soap|seoul white.*soap|gluta.*soap|kojie san.*soap|kojiesan|active white.*soap|ar soap|beauche.*soap|dr alvin kojic soap|dr wong|bevi body soap|nature glutathione|kawaii max glutathion|symply g kojic soap|vet core active soap|seaglow spicy sauce|defensil|jb soap|rexona)\b', "Personal Care - Body"),
    (r'\b(toothpaste|toothbrush|mouthwash|colgate|closeup|oral.?b|listerine|sansflou|sansfluo|polident|dentiste)\b', "Personal Care - Oral"),
    (r'\b(facial wash|toner|sunscreen|face cream|serum|olay|cetaphil|neutrogena|pond\b|clean.?clear|garnier.*facial|garnier.*men|gatsby facial|gatsby.*scrub|senka|celeteque|skin white|iwhite|belo\b|hello glow nose|hello glow lip|beauty formulas|k.?glow ice scrub|gt bleaching|ever bilena|maybelline|nail polish|nail tips|fake nail|omg.*nail|bobbie nail|my nails nail|watsup50 nail|maxi beauty astringent|cathydoll facial|cathydoll ffoam|barones aloe vera sun cream|dermplus|baroness aloe)\b', "Personal Care - Facial"),
    (r'\b(feminine wash|feminine spray|lactacyd feminine|ph care|gluta c feminine|naflora|betadine antiseptic feminine|hers feminine wipes|charmee menstrual|sisters menstrual|modess|sofy|secure men pads)\b', "Personal Care - Feminine"),
    (r'\b(cologne|perfume|body mist|ellips cologne|juicy cologne|bench daily scent|nenuco|naturelab cologne|naturelab bt21 fragrance|herbench|babyflo cologne)\b', "Personal Care - Fragrance"),
    # Pet Care
    (r'\b(dog food|cat food|pedigree|whiskas|purina|alpo|minino|goodest catfood|moochie|nutripet|doggo|lucky dog|dentalight|pet plus dental|top2tail|topdog|playpets|doggies choice|petgard|our cat|easy scoop cat|bayopet|woof n tail|kg pet round|calming bed|pet bed|foam bag ripstop|shield gard flea and tick)\b', "Pet Care"),
    # Ready to Cook / Processed Meats
    (r'\b(ham\b|bacon|jamon|chorizo de bilbao|chorizo de pamplona|holiday ham|hamonado|hotdog|sausage\b|corned dog|franks|tocino\b|longganisa|hungarian sausage|salami|pepperoni|bratwurst|schublig|cabanossi|debreziner|nuremberg|vienna sausage|meatloaf|meatball|frankfurter|cdo idol|cdo jamon|cdo karne|cdo crispy|cdo ulam|cdo holiday|cdo premium|star nm sausage|star nm ulam|virginia|winner hamonado|winner hotdog|purefoods tender juicy|purefoods fs|bounty fresh|fat.*thin.*sausage|fat.*thin.*longanisa|kwong bee sausage|black bridge sausage|sajo cocktail|stefan.*sausage|stefan.*salami|stefan.*bratwurst|stefan.*meatloaf|king sue|belcris|bellshayce|aguila gourmet|aguila chorizo|aguila hungarian|el rancho burger|cdohamburger|purefoods.*burger|food service.*patty|food service.*corndog|tapa king|star nm ulam)\b', "Ready to Cook - Processed Meats"),
    (r'\b(ulam\b|humba|kare.?kare|caldereta|menudo|mechado|afritada|pinakbet|paksiw|bistek|bicols best|goldilocks laing|ks specialty sisig|max.?s crispy pata|tapa\b)\b', "Ready to Cook - Filipino"),
    (r'\brtc\b|ready.?to.?cook|marinated|breaded', "Ready to Cook - Marinated"),
    # Non-Food
    (r'\b(kitchenware|dispenser|container|storage|utensil|plate\b|mug\b|spatula|ladle\b|tong\b|watts houseware|watts accessories|watts baking|watts stationery|watts diy|watsup50|creazions shoe cabinet|astron blender|astron flat iron|astron multi pot|hanabishi blender|eureka dry flat iron|eureka microwave|tough mama|toughmama|tenki cables|tenki razor|ramgo microgreens|ramgo packet|ramgo rosemary|ramgo sprout|aster chair|pumpkin pop|christmas tree tops|christmas snowman|christmas.*box|christmas.*house|christmas.*plastic|christmas.*socks|christmas.*book|dutche.*holiday|dutche.*valentine)\b', "Non-Food - Household"),
    # Gift Sets
    (r'\b(fruit basket|gift set|hamper|promo pack|bundle|assorted fruits 13 kinds)\b', "Promos & Gift Sets"),
]



# =============================================================================
# HELPERS
# =============================================================================

def parse_unit_size(name: str) -> str:
    """
    Extracts unit/size info from a product name string using regex.

    Parameters
    ----------
    name : str
        Raw product name e.g. 'Nestle Fresh Milk 1L'

    Returns
    -------
    str
        Extracted size string e.g. '1L', or '' if none found.

    Examples
    --------
    >>> parse_unit_size("Magnolia Fresh Milk 1L")
    '1L'
    """
    pattern = (
        r'\b(\d+(\.\d+)?\s?(kg|g|ml|L|l|pcs|pc|pack|sachet|pouch|strips?|tabs?|capsules?)'
        r'|(\d+x\d+(\.\d+)?\s?(ml|g|L|l|kg)))\b'
    )
    match = re.search(pattern, name, re.IGNORECASE)
    return match.group(0).strip() if match else ""


def parse_brand(name: str) -> str:
    """
    Returns the first word of a product name as a heuristic brand extraction.

    Parameters
    ----------
    name : str
        Raw product name.

    Returns
    -------
    str
        First token, assumed to be the brand name.
    """
    tokens = name.strip().split()
    return tokens[0] if tokens else ""


def assign_category(item_name: str) -> str:
    """
    Assigns a category to a product based on keyword matching in its name.

    Iterates through NAME_RULES in order (most specific first) and returns
    the first matching category. Returns 'Uncategorized' if no match found.

    Parameters
    ----------
    item_name : str
        Product name e.g. 'Nescafe 3in1 Original | 10x20g'

    Returns
    -------
    str
        Category name e.g. 'Beverage - Coffee & Tea'
    """
    name = item_name.lower()
    for pattern, category in NAME_RULES:
        if re.search(pattern, name, re.IGNORECASE):
            return category
    return "Uncategorized"


# =============================================================================
# SITEMAP PARSER
# =============================================================================

def fetch_products_from_sitemap(sitemap_url: str) -> list[dict]:
    """
    Parses the SM Markets product sitemap XML to get all product URLs and names.

    The sitemap already contains the product name in <image:title> so no
    extra page visit is needed just for the name.

    Parameters
    ----------
    sitemap_url : str
        URL of the product sitemap XML.

    Returns
    -------
    list of dict
        Each dict has keys: 'url' (str), 'item_name' (str).
    """
    print("Fetching product list from sitemap...")
    resp = requests.get(sitemap_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    ns = {
        "sm":    "http://www.sitemaps.org/schemas/sitemap/0.9",
        "image": "http://www.google.com/schemas/sitemap-image/1.1",
    }
    root = ET.fromstring(resp.content)
    products = []

    for url_el in root.findall("sm:url", ns):
        loc = url_el.findtext("sm:loc", namespaces=ns) or ""
        title_el = url_el.find("image:image/image:title", ns)
        item_name = (
            title_el.text.strip()
            if title_el is not None and title_el.text
            else ""
        )
        if loc and item_name:
            products.append({"url": loc, "item_name": item_name})

    print(f"Found {len(products)} products in sitemap.")
    return products


# =============================================================================
# SELENIUM DRIVER
# =============================================================================

def create_driver() -> webdriver.Edge:
    """
    Creates a headless Edge WebDriver using the local msedgedriver.exe.
    Images are disabled to speed up page loads.
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Edge(service=Service(DRIVER_PATH), options=options)
    driver.implicitly_wait(5)
    return driver


def fetch_price_selenium(driver: webdriver.Edge, url: str) -> float | None:
    """
    Visits a product page and extracts the price using the rendered CSS class.

    SM Markets renders price in a div with class matching 'productFullDetail-price'.
    We wait until that element appears (fast, stops as soon as it loads)
    then extract the ₱ value from it.

    Parameters
    ----------
    driver : webdriver.Edge
        Active Selenium WebDriver instance.
    url : str
        Full product page URL.

    Returns
    -------
    float or None
        Price in PHP, or None if not found.
    """
    try:
        driver.get(url)

        # wait until the price div appears — class contains 'productFullDetail-price'
        el = WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[class*='productFullDetail-price']")
            )
        )
        raw = el.text.strip()
        raw = raw.replace(",", "").replace("₱", "").strip()
        match = re.search(r'\d+(\.\d+)?', raw)
        if match:
            return float(match.group(0))

    except Exception:
        pass
    return None


# =============================================================================
# MAIN SCRAPER
# =============================================================================

class GroceryScraper:
    """
    Scrapes grocery product data from SM Markets (smmarkets.ph).

    Strategy:
    1. Parse sitemap-1-2.xml to get all product names instantly (no browser).
    2. Assign category from product name using keyword rules.
    3. Use Selenium to visit each product page and fetch the price.
       (requests is blocked with 403 by smmarkets.ph)

    Parameters
    ----------
    output_path : str
        Path where the final CSV will be saved.
    """

    def __init__(self, output_path: str = OUTPUT_PATH):
        self.output_path = output_path

    def execute(self) -> pd.DataFrame:
        """
        Runs the full scraping pipeline.

        Steps:
        1. Parse product sitemap for names and URLs.
        2. Assign category by keyword matching on product name.
        3. Use Selenium to visit each product page and fetch the price.
        4. Assemble and save the final DataFrame.

        Returns
        -------
        pd.DataFrame
            Dataset with columns: item_category, item_name, brand,
            unit_size, price_php, store, date_scraped.
        """
        os.makedirs(
            os.path.dirname(self.output_path)
            if os.path.dirname(self.output_path) else ".",
            exist_ok=True
        )
        today = str(date.today())

        # step 1: get all product names from sitemap
        products = fetch_products_from_sitemap(SITEMAP_PRODUCTS)

        # step 2: assign categories from product names
        for p in products:
            p["item_category"] = assign_category(p["item_name"])

        categorized = sum(
            1 for p in products if p["item_category"] != "Uncategorized"
        )
        print(f"Categorized: {categorized} / {len(products)} from name matching.")

        # step 3: fetch prices with NUM_BROWSERS parallel Edge instances
        print(f"Launching {NUM_BROWSERS} parallel browsers for {len(products)} products...")
        est = len(products) // NUM_BROWSERS * 2 // 60
        print(f"Estimated time: ~{est} mins")

        price_map = {}
        lock = threading.Lock()
        progress = tqdm(total=len(products), desc="Fetching prices")

        def worker(chunk: list) -> None:
            """Each thread owns one Edge browser and processes its product chunk."""
            driver = create_driver()
            try:
                for p in chunk:
                    price = fetch_price_selenium(driver, p["url"])
                    with lock:
                        price_map[p["url"]] = price
                        progress.update(1)
                        # checkpoint every 500 products fetched across all browsers
                        if len(price_map) % 500 == 0:
                            snap = [{
                                "item_category": pp["item_category"],
                                "item_name":     pp["item_name"],
                                "brand":         parse_brand(pp["item_name"]),
                                "unit_size":     parse_unit_size(pp["item_name"]),
                                "price_php":     price_map.get(pp["url"]),
                                "store":         STORE_NAME,
                                "date_scraped":  today,
                            } for pp in products if pp["url"] in price_map]
                            pd.DataFrame(snap).to_csv(
                                self.output_path + ".checkpoint.csv",
                                index=False, encoding="utf-8-sig"
                            )
                    time.sleep(POLITE_DELAY)
            finally:
                driver.quit()

        # split products evenly across browsers
        chunk_size = len(products) // NUM_BROWSERS + 1
        chunks = [products[i:i + chunk_size] for i in range(0, len(products), chunk_size)]

        threads = [threading.Thread(target=worker, args=(c,)) for c in chunks]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        progress.close()

        records = [{
            "item_category": p["item_category"],
            "item_name":     p["item_name"],
            "brand":         parse_brand(p["item_name"]),
            "unit_size":     parse_unit_size(p["item_name"]),
            "price_php":     price_map.get(p["url"]),
            "store":         STORE_NAME,
            "date_scraped":  today,
        } for p in products]

        df = pd.DataFrame(records)
        df.sort_values(["item_category", "item_name"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(self.output_path, index=False, encoding="utf-8-sig")

        print(f"\nDone! {len(df)} products saved to: {self.output_path}")
        print(f"Unique categories: {df['item_category'].nunique()}")
        print(f"Products with price: {df['price_php'].notna().sum()}")

        return df


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    scraper = GroceryScraper()
    df = scraper.execute()
    print(df.head(10).to_string())
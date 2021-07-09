import requests
import json
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout, ConnectionError
from lxml import html
from collections import defaultdict
from math import ceil
from variables import required_data, headers, HOME_URL, HOME_URL1, PRODUCTS_PER_PAGE


def make_http_request(url, *args, headers=headers, method='GET', type_of_result='JSON'):
    """Makes http_request for specific sets of parameters."""

    homedepot_adapter = HTTPAdapter(max_retries=5)
    session = requests.Session()
    session.mount('https://', homedepot_adapter)
    try:
        if method == "GET":
            data = session.get(url, headers=headers, timeout=30)
            if type_of_result == 'JSON':
                json_data = json.loads(data.content)
                return json_data
            else:
                return data
        else:
            payload, head, querystring = args
            response = requests.post(url, data=payload, headers=head, params=querystring)
            return response
    except ConnectionError as ce:
        print(ce)


def get_navigation():
    """Returns a json with navigation.

        URL to json with navigation is hardcoded and has been taken from dev-tools.
    """

    print("Robot is running and scraping a new dataset from homedepot..")
    url = HOME_URL + f'hdus/en_US/DTCCOMNEW/fetch/headerFooterFlyout-8.json'
    return make_http_request(url)


def parse_navigation(full_nav):
    """Returns a parsed navigation as a json.

    :param full_nav: full navigation as a json, data needs to be parsed.
    :return: parsed_navigation: parsed navigation, contains only json with chosen dep/cat/sub.
    """

    taxonomies = required_data['navigation']
    parsed_navigation = defaultdict(list)
    temp_navigation = {}
    homedepot_taxonomy = full_nav['header']['primaryNavigation']
    for taxonomy in taxonomies:
        required_dep = taxonomy['department']
        required_cat = taxonomy['category_name']
        required_subcat = taxonomy['sub_category_name']
        required_brand = taxonomy['brands']
        required_store_id = taxonomy['store_ids']
        required_delivery_zip = taxonomy['delivery_zip']
        for department in homedepot_taxonomy:
            department_title = department['title']
            categories_list = department['l2']
            if department_title == required_dep:
                department_name = department_title
                for category in categories_list:
                    category_title = category['name']
                    if category_title == required_cat:
                        category_name = category_title
                        category_url = HOME_URL + category['url']
                        category_url = category_url.replace('SECURE_SUPPORTED/', '')
                        if required_subcat != '':
                            subcategory_list = category['l3']
                            for subcategory in subcategory_list:
                                subcategory_title = subcategory['name']
                                if subcategory_title == required_subcat:
                                    subcategory_name = subcategory_title
                                    subcategory_url = HOME_URL + subcategory['url']
                                    subcategory_url = subcategory_url.replace('SECURE_SUPPORTED/', '')
                        for brand in required_brand:
                            brand_name = brand
                            temp_navigation['department_name'] = department_name
                            temp_navigation['category_name'] = category_name
                            temp_navigation['brand'] = brand_name
                            if required_subcat == '':
                                temp_navigation['url'] = category_url
                            else:
                                temp_navigation['url'] = subcategory_url
                                temp_navigation['subcategory_name'] = subcategory_name
                            for idx, store_id in enumerate(required_store_id):
                                temp_navigation['store_id'] = store_id
                                temp_navigation['delivery_zip'] = required_delivery_zip[idx]
                                temp_navigation_copy = temp_navigation.copy()
                                parsed_navigation['navigation'].append(temp_navigation_copy)
    return json.dumps(parsed_navigation, indent=4)


def extract_url(nav):
    """Each of brand has different URL. This method is scraping URL from html and assigning to specific taxonomy.

    :param nav: parsed navigation but url is missing.
    :return: ready_navigation: ready navigation to use.
    """

    taxonomies = json.loads(nav)['navigation']
    parsed_navigation = defaultdict(list)
    temp_nav = {}
    for taxonomy in taxonomies:
        temp_nav['department_name'] = taxonomy['department_name']
        temp_nav['category_name'] = taxonomy['category_name']
        if 'subcategory_name' in taxonomy.keys():
            temp_nav['subcategory_name'] = taxonomy['subcategory_name']
        brand_name = taxonomy['brand']
        listing_url = taxonomy['url']
        http_request = make_http_request(listing_url, headers=headers, type_of_result='HTML')
        tree = html.fromstring(http_request.content)
        urls = tree.xpath("//div[@class='EtchCustomNavigation etch-analytics']//li[@class='list__item--padding-none']//a/@href")
        brands = tree.xpath("//div[@class='EtchCustomNavigation etch-analytics']//li[@class='list__item--padding-none']//a[@href]//text()")
        if len(brands) == 0:
            brand_url = tree.xpath('//div[@class="grid-column  col__3-12 col__2-12--xs col__3-12--sm col__3-12--md col__3-12--lg recursive-content"]//a[contains(@href,"'+brand_name+'")]/@href')[0]
            brand_url = HOME_URL1 + brand_url
        if brand_name in brands:
            brand_idx = brands.index(brand_name)
            brand_url = HOME_URL1 + urls[brand_idx]
        temp_nav['brand_name'] = brand_name
        temp_nav['url'] = brand_url
        idx = brand_url.rfind('/')
        temp_nav_param = brand_url[idx+1:]
        idx = temp_nav_param.rfind('N-')
        temp_nav['nav_param'] = temp_nav_param[idx + 2:]
        temp_nav['store_id'] = taxonomy['store_id']
        temp_nav['delivery_zip'] = taxonomy['delivery_zip']
        temp_nav_copy = temp_nav.copy()
        parsed_navigation['navigation'].append(temp_nav_copy)

    return json.dumps(parsed_navigation, indent=4)


def pagination(nav_param, page, store_id, delivery_zip, referer):
    """It makes pagination for specific set of taxonomy.

    :param nav_param: required to paylod
    :param page: page
    :param store_id: id of store
    :param delivery_zip: zip_code
    :param referer: required to headers
    """

    page += 1
    start_index = (PRODUCTS_PER_PAGE * page) - PRODUCTS_PER_PAGE
    url = HOME_URL + 'federation-gateway/graphql?opname=searchModel'
    querystring = {"opname": "searchModel"}
    payload = "{\"operationName\":\"searchModel\",\"variables\":{\"skipInstallServices\":false,\"skipKPF\":false,\"skipSpecificationGroup\":false,\"storefilter\":\"ALL\",\"channel\":\"DESKTOP\",\"additionalSearchParams\":{\"deliveryZip\":\"04401\"},\"filter\":{},\"navParam\":\"5yc1vZc3piZa0f\",\"orderBy\":{\"field\":\"TOP_SELLERS\",\"order\":\"ASC\"},\"pageSize\":24,\"startIndex\":0,\"storeId\":\"2414\"},\"query\":\"query searchModel($startIndex: Int, $pageSize: Int, $orderBy: ProductSort, $filter: ProductFilter, $storeId: String, $zipCode: String, $skipInstallServices: Boolean = true, $skipKPF: Boolean = false, $skipSpecificationGroup: Boolean = false, $keyword: String, $navParam: String, $storefilter: StoreFilter = ALL, $itemIds: [String], $channel: Channel = DESKTOP, $additionalSearchParams: AdditionalParams, $loyaltyMembershipInput: LoyaltyMembershipInput) {\\n  searchModel(keyword: $keyword, navParam: $navParam, storefilter: $storefilter, storeId: $storeId, itemIds: $itemIds, channel: $channel, additionalSearchParams: $additionalSearchParams, loyaltyMembershipInput: $loyaltyMembershipInput) {\\n    metadata {\\n      categoryID\\n      analytics {\\n        semanticTokens\\n        dynamicLCA\\n        __typename\\n      }\\n      canonicalUrl\\n      searchRedirect\\n      clearAllRefinementsURL\\n      contentType\\n      cpoData {\\n        cpoCount\\n        cpoOnly\\n        totalCount\\n        __typename\\n      }\\n      isStoreDisplay\\n      productCount {\\n        inStore\\n        __typename\\n      }\\n      stores {\\n        storeId\\n        storeName\\n        address {\\n          postalCode\\n          __typename\\n        }\\n        nearByStores {\\n          storeId\\n          storeName\\n          distance\\n          address {\\n            postalCode\\n            __typename\\n          }\\n          __typename\\n        }\\n        __typename\\n      }\\n      __typename\\n    }\\n    products(startIndex: $startIndex, pageSize: $pageSize, orderBy: $orderBy, filter: $filter) {\\n      identifiers {\\n        storeSkuNumber\\n        canonicalUrl\\n        brandName\\n        itemId\\n        productLabel\\n        modelNumber\\n        productType\\n        parentId\\n        isSuperSku\\n        __typename\\n      }\\n      itemId\\n      dataSources\\n      media {\\n        images {\\n          url\\n          type\\n          subType\\n          sizes\\n          __typename\\n        }\\n        __typename\\n      }\\n      pricing(storeId: $storeId) {\\n        value\\n        alternatePriceDisplay\\n        alternate {\\n          bulk {\\n            pricePerUnit\\n            thresholdQuantity\\n            value\\n            __typename\\n          }\\n          unit {\\n            caseUnitOfMeasure\\n            unitsOriginalPrice\\n            unitsPerCase\\n            value\\n            __typename\\n          }\\n          __typename\\n        }\\n        original\\n        mapAboveOriginalPrice\\n        message\\n        preferredPriceFlag\\n        promotion {\\n          type\\n          description {\\n            shortDesc\\n            longDesc\\n            __typename\\n          }\\n          dollarOff\\n          percentageOff\\n          savingsCenter\\n          savingsCenterPromos\\n          specialBuySavings\\n          specialBuyDollarOff\\n          specialBuyPercentageOff\\n          dates {\\n            start\\n            end\\n            __typename\\n          }\\n          __typename\\n        }\\n        specialBuy\\n        unitOfMeasure\\n        __typename\\n      }\\n      reviews {\\n        ratingsReviews {\\n          averageRating\\n          totalReviews\\n          __typename\\n        }\\n        __typename\\n      }\\n      availabilityType {\\n        discontinued\\n        type\\n        __typename\\n      }\\n      badges(storeId: $storeId) {\\n        name\\n        __typename\\n      }\\n      details {\\n        collection {\\n          collectionId\\n          name\\n          url\\n          __typename\\n        }\\n        __typename\\n      }\\n      favoriteDetail {\\n        count\\n        __typename\\n      }\\n      fulfillment(storeId: $storeId, zipCode: $zipCode) {\\n        backordered\\n        backorderedShipDate\\n        seasonStatusEligible\\n        fulfillmentOptions {\\n          type\\n          fulfillable\\n          services {\\n            type\\n            hasFreeShipping\\n            freeDeliveryThreshold\\n            locations {\\n              curbsidePickupFlag\\n              isBuyInStoreCheckNearBy\\n              distance\\n              inventory {\\n                isOutOfStock\\n                isInStock\\n                isLimitedQuantity\\n                isUnavailable\\n                quantity\\n                maxAllowedBopisQty\\n                minAllowedBopisQty\\n                __typename\\n              }\\n              isAnchor\\n              locationId\\n              storeName\\n              type\\n              __typename\\n            }\\n            __typename\\n          }\\n          __typename\\n        }\\n        __typename\\n      }\\n      info {\\n        isBuryProduct\\n        isSponsored\\n        isGenericProduct\\n        isLiveGoodsProduct\\n        sponsoredBeacon {\\n          onClickBeacon\\n          onViewBeacon\\n          __typename\\n        }\\n        sponsoredMetadata {\\n          campaignId\\n          placementId\\n          slotId\\n          __typename\\n        }\\n        globalCustomConfigurator {\\n          customExperience\\n          __typename\\n        }\\n        returnable\\n        hidePrice\\n        productSubType {\\n          name\\n          link\\n          __typename\\n        }\\n        categoryHierarchy\\n        samplesAvailable\\n        customerSignal {\\n          previouslyPurchased\\n          __typename\\n        }\\n        productDepartmentId\\n        productDepartment\\n        augmentedReality\\n        ecoRebate\\n        quantityLimit\\n        sskMin\\n        sskMax\\n        unitOfMeasureCoverage\\n        wasMaxPriceRange\\n        wasMinPriceRange\\n        swatches {\\n          isSelected\\n          itemId\\n          label\\n          swatchImgUrl\\n          url\\n          value\\n          __typename\\n        }\\n        totalNumberOfOptions\\n        paintBrand\\n        __typename\\n      }\\n      installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\\n        scheduleAMeasure\\n        __typename\\n      }\\n      keyProductFeatures @skip(if: $skipKPF) {\\n        keyProductFeaturesItems {\\n          features {\\n            name\\n            refinementId\\n            refinementUrl\\n            value\\n            __typename\\n          }\\n          __typename\\n        }\\n        __typename\\n      }\\n      specificationGroup @skip(if: $skipSpecificationGroup) {\\n        specifications {\\n          specName\\n          specValue\\n          __typename\\n        }\\n        specTitle\\n        __typename\\n      }\\n      sizeAndFitDetail {\\n        attributeGroups {\\n          attributes {\\n            attributeName\\n            dimensions\\n            __typename\\n          }\\n          dimensionLabel\\n          productType\\n          __typename\\n        }\\n        __typename\\n      }\\n      __typename\\n    }\\n    id\\n    searchReport {\\n      totalProducts\\n      didYouMean\\n      correctedKeyword\\n      keyword\\n      pageSize\\n      searchUrl\\n      sortBy\\n      sortOrder\\n      startIndex\\n      __typename\\n    }\\n    relatedResults {\\n      universalSearch {\\n        title\\n        __typename\\n      }\\n      relatedServices {\\n        label\\n        __typename\\n      }\\n      visualNavs {\\n        label\\n        imageId\\n        webUrl\\n        categoryId\\n        imageURL\\n        __typename\\n      }\\n      visualNavContainsEvents\\n      relatedKeywords {\\n        keyword\\n        __typename\\n      }\\n      __typename\\n    }\\n    taxonomy {\\n      brandLinkUrl\\n      breadCrumbs {\\n        browseUrl\\n        creativeIconUrl\\n        deselectUrl\\n        dimensionId\\n        dimensionName\\n        label\\n        refinementKey\\n        url\\n        __typename\\n      }\\n      __typename\\n    }\\n    templates\\n    partialTemplates\\n    dimensions {\\n      label\\n      refinements {\\n        refinementKey\\n        label\\n        recordCount\\n        selected\\n        imgUrl\\n        url\\n        nestedRefinements {\\n          label\\n          url\\n          recordCount\\n          refinementKey\\n          __typename\\n        }\\n        __typename\\n      }\\n      collapse\\n      dimensionId\\n      isVisualNav\\n      isVisualDimension\\n      nestedRefinementsLimit\\n      visualNavSequence\\n      __typename\\n    }\\n    orangeGraph {\\n      universalSearchArray {\\n        pods {\\n          title\\n          description\\n          imageUrl\\n          link\\n          __typename\\n        }\\n        info {\\n          title\\n          __typename\\n        }\\n        __typename\\n      }\\n      productTypes\\n      __typename\\n    }\\n    appliedDimensions {\\n      label\\n      refinements {\\n        label\\n        refinementKey\\n        url\\n        __typename\\n      }\\n      __typename\\n    }\\n    aisles {\\n      shopAllUrl\\n      aisleCategory\\n      aisleBrand\\n      aislePosition\\n      aisle {\\n        name\\n        displayName\\n        shopAllUrl\\n        products {\\n          itemId\\n          dataSources\\n          pricing(storeId: $storeId) {\\n            original\\n            value\\n            message\\n            promotion {\\n              type\\n              description {\\n                shortDesc\\n                longDesc\\n                __typename\\n              }\\n              dollarOff\\n              percentageOff\\n              savingsCenter\\n              savingsCenterPromos\\n              specialBuySavings\\n              specialBuyDollarOff\\n              specialBuyPercentageOff\\n              dates {\\n                start\\n                end\\n                __typename\\n              }\\n              __typename\\n            }\\n            alternatePriceDisplay\\n            alternate {\\n              bulk {\\n                pricePerUnit\\n                thresholdQuantity\\n                value\\n                __typename\\n              }\\n              unit {\\n                caseUnitOfMeasure\\n                unitsOriginalPrice\\n                unitsPerCase\\n                value\\n                __typename\\n              }\\n              __typename\\n            }\\n            mapAboveOriginalPrice\\n            preferredPriceFlag\\n            specialBuy\\n            unitOfMeasure\\n            __typename\\n          }\\n          reviews {\\n            ratingsReviews {\\n              averageRating\\n              totalReviews\\n              __typename\\n            }\\n            __typename\\n          }\\n          media {\\n            images {\\n              url\\n              sizes\\n              type\\n              subType\\n              __typename\\n            }\\n            __typename\\n          }\\n          identifiers {\\n            itemId\\n            productLabel\\n            canonicalUrl\\n            brandName\\n            modelNumber\\n            productType\\n            storeSkuNumber\\n            parentId\\n            isSuperSku\\n            __typename\\n          }\\n          availabilityType {\\n            discontinued\\n            type\\n            __typename\\n          }\\n          badges(storeId: $storeId) {\\n            name\\n            __typename\\n          }\\n          details {\\n            collection {\\n              collectionId\\n              name\\n              url\\n              __typename\\n            }\\n            __typename\\n          }\\n          favoriteDetail {\\n            count\\n            __typename\\n          }\\n          fulfillment(storeId: $storeId, zipCode: $zipCode) {\\n            backordered\\n            backorderedShipDate\\n            seasonStatusEligible\\n            fulfillmentOptions {\\n              type\\n              fulfillable\\n              services {\\n                type\\n                hasFreeShipping\\n                freeDeliveryThreshold\\n                locations {\\n                  curbsidePickupFlag\\n                  isBuyInStoreCheckNearBy\\n                  distance\\n                  inventory {\\n                    isOutOfStock\\n                    isInStock\\n                    isLimitedQuantity\\n                    isUnavailable\\n                    quantity\\n                    maxAllowedBopisQty\\n                    minAllowedBopisQty\\n                    __typename\\n                  }\\n                  isAnchor\\n                  locationId\\n                  storeName\\n                  type\\n                  __typename\\n                }\\n                __typename\\n              }\\n              __typename\\n            }\\n            __typename\\n          }\\n          info {\\n            isBuryProduct\\n            isSponsored\\n            isGenericProduct\\n            isLiveGoodsProduct\\n            sponsoredBeacon {\\n              onClickBeacon\\n              onViewBeacon\\n              __typename\\n            }\\n            sponsoredMetadata {\\n              campaignId\\n              placementId\\n              slotId\\n              __typename\\n            }\\n            globalCustomConfigurator {\\n              customExperience\\n              __typename\\n            }\\n            returnable\\n            hidePrice\\n            productSubType {\\n              name\\n              link\\n              __typename\\n            }\\n            categoryHierarchy\\n            samplesAvailable\\n            customerSignal {\\n              previouslyPurchased\\n              __typename\\n            }\\n            productDepartmentId\\n            productDepartment\\n            augmentedReality\\n            ecoRebate\\n            quantityLimit\\n            sskMin\\n            sskMax\\n            unitOfMeasureCoverage\\n            wasMaxPriceRange\\n            wasMinPriceRange\\n            swatches {\\n              isSelected\\n              itemId\\n              label\\n              swatchImgUrl\\n              url\\n              value\\n              __typename\\n            }\\n            totalNumberOfOptions\\n            __typename\\n          }\\n          installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\\n            scheduleAMeasure\\n            __typename\\n          }\\n          keyProductFeatures @skip(if: $skipKPF) {\\n            keyProductFeaturesItems {\\n              features {\\n                name\\n                refinementId\\n                refinementUrl\\n                value\\n                __typename\\n              }\\n              __typename\\n            }\\n            __typename\\n          }\\n          specificationGroup @skip(if: $skipSpecificationGroup) {\\n            specifications {\\n              specName\\n              specValue\\n              __typename\\n            }\\n            specTitle\\n            __typename\\n          }\\n          sizeAndFitDetail {\\n            attributeGroups {\\n              attributes {\\n                attributeName\\n                dimensions\\n                __typename\\n              }\\n              dimensionLabel\\n              productType\\n              __typename\\n            }\\n            __typename\\n          }\\n          __typename\\n        }\\n        __typename\\n      }\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\"}"
    if page > 1:
        payload = payload.replace('\"startIndex\":0', f'\"startIndex\":{start_index}')
    payload = payload.replace('\"storeId\":"2414"', f'\"storeId\":"{store_id}"')
    payload = payload.replace('\"deliveryZip\":"04401"', f'\"deliveryZip\":"{delivery_zip}"')
    payload = payload.replace('\"navParam\":\"5yc1vZc3piZa0f\"', f'\"navParam\":"{nav_param}"')
    head = {
            'authority': "www.homedepot.com",
            'sec-ch-ua': "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"91\", \"Chromium\";v=\"91\"",
            'apollographql-client-name': "major-appliances",
            'x-debug': "false",
            'sec-ch-ua-mobile': "?0",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
            'content-type': "application/json",
            'accept': "*/*",
            'x-cloud-trace-context': "1e0083ee5406ca057ca891318ca44d47/1958852399023475434",
            'apollographql-client-version': "0.0.0",
            'x-api-cookies': "{\"x-user-id\":\"78cd21c3-ba39-8c61-5449-cd6a2e849784\"}",
            'x-current-url': "/b/Appliances-Refrigerators/Samsung/N-5yc1vZc3piZa0f",
            'x-experience-name': "major-appliances",
            'origin': "https://www.homedepot.com",
            'sec-fetch-site': "same-origin",
            'sec-fetch-mode': "cors",
            'sec-fetch-dest': "empty",
            'referer': f"{referer}",
            'accept-language': "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            'cookie': "HD_DC=origin; THD_NR=0; THD_SESSION=; THD_CACHE_NAV_SESSION=; THD_CACHE_NAV_PERSIST=; WORKFLOW=GEO_LOCATION; THD_FORCE_LOC=1; THD_INTERNAL=0; DELIVERY_ZIP=04401; DELIVERY_ZIP_TYPE=DEFAULT; thda.s=52a7a2a7-239b-9d37-11b5-7ae5ebc611c8; thda.u=78cd21c3-ba39-8c61-5449-cd6a2e849784; operation_id=8d642a62-50ac-4e0d-bd34-6609589663eb; at_check=true; AMCVS_F6421253512D2C100A490D45%40AdobeOrg=1; AMCV_F6421253512D2C100A490D45%40AdobeOrg=1585540135%7CMCIDTS%7C18816%7CMCMID%7C46021052815257554615654198431075459321%7CMCOPTOUT-1625669412s%7CNONE%7CvVersion%7C4.4.0; mp_0e3ea14e7e90fc91592bf29cb9917ec6_mixpanel=%7B%22distinct_id%22%3A%20%2217a8104d22a309-066a10e209c19a-3373266-144000-17a8104d22b44c%22%2C%22%24device_id%22%3A%20%2217a8104d22a309-066a10e209c19a-3373266-144000-17a8104d22b44c%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; LOCALIZED_STORE_INFO={%22zipcode%22:%2204401%22%2C%22storeId%22:%222414%22%2C%22state%22:%22ME%22%2C%22name%22:%22Bangor%22%2C%22city%22:%22Bangor%22}; mbox=session#32db42160a194d01bdb18b20e41e5a10#1625664073|PC#32db42160a194d01bdb18b20e41e5a10.37_0#1688907015; forterToken=9fdd00709f934aa5a669004ff869c27a_1625662221336_263_UDF9u_13ck; bm_sz=7C0F1BCBC4A39D9F283230885406B814~YAAQnmReaJaIqHl6AQAA3goxggyZ4U6N65nlEQlhWESNnHWIOvjEhD8TXjNkXLxQTbCxk50/hniQj4HpWaDXOVDmUeC+to0eik4lAA3oDh37WfsYtCj4q/CpxP01CnXArL9OYFV1QtLGZw/iZkdKgYCXk1faPPS6m66WTLPmRX+BSTTxKbbn3OQ6kzv+qP4VAQAa; AKA_A2=A; ak_bmsc=DC63056482C406A220FE3CB2A236711A~000000000000000000000000000000~YAAQrWReaNhN9oF6AQAA+mzjggwkbfiUlHaOCdEFd9m0fkQo3WyMNR4E1rSHtipu6aGzsWwmtNc0MKFnw3YHB8eg5aPN8vxYyVnnRO6MxKrqU5pVMZmtfrvRYdQHCy0+KG8I5TBDayD2qIyfgmNLbhc4mKdGFFRztxfWm4xFWgM3ZgIneLR0QkdfdNnWnOx8bM/b5JNkvacwkAb88acq68/9jPGWPWZTAqxYtqFKML3lF4mfTcaI4zJVXA4LyKO1K1xnMp0ODFUx8gUPh6TN6wyf1sZjUnZ9c3G1pdkJSqgrKYZz00SuEdWsxuRtHlnpTNMGyKxhl5SleImhWs6VKDqYpqqwYnbHbIuUIT+lu9XaQbfCEgFeepadd7H5ZAH5BlsGph9NJ3W9rMvPd8I=; THD_PERSIST=C4%3D2414%2BBangor%20-%20Bangor%2C%20ME%2B%3A%3BC4_EXP%3D1657177631%3A%3BC24%3D04401%3A%3BC24_EXP%3D1657177631%3A%3BC39%3D1%3B8%3A00-20%3A00%3B2%3B6%3A00-21%3A00%3B3%3B6%3A00-21%3A00%3B4%3B6%3A00-21%3A00%3B5%3B6%3A00-21%3A00%3B6%3B6%3A00-21%3A00%3B7%3B6%3A00-21%3A00%3A%3BC39_EXP%3D1625697181; THD_LOCALIZER=%7B%22WORKFLOW%22%3A%22GEO_LOCATION%22%2C%22THD_FORCE_LOC%22%3A%221%22%2C%22THD_INTERNAL%22%3A%220%22%2C%22THD_STRFINDERZIP%22%3A%2204401%22%2C%22THD_LOCSTORE%22%3A%222414%2BBangor%20-%20Bangor%2C%20ME%2B%22%2C%22THD_STORE_HOURS%22%3A%221%3B8%3A00-20%3A00%3B2%3B6%3A00-21%3A00%3B3%3B6%3A00-21%3A00%3B4%3B6%3A00-21%3A00%3B5%3B6%3A00-21%3A00%3B6%3B6%3A00-21%3A00%3B7%3B6%3A00-21%3A00%22%2C%22THD_STORE_HOURS_EXPIRY%22%3A1625697181%7D; ecrSessionId=EC4E3364B81B2F0F8C631F939B0DF489; _abck=9AAE0E4F30306E66F328979EB7BAB4F7~0~YAAQrWReaNyW9oF6AQAAxArsggZYRtd1UkZsnMo5LhPYIps3Jj/7T0xr4W9QVk4bgIsUChVq0CyQj67UUSmcXhxdMk4aqKr33YnWEIXtboTGcnTuZiCVuiCVhr0jzBrmbRLKKd5HFwt1WQAG9UjgM34Xc18RzQDCj6osGyVIiBVGLmqQlliMzKGMODrKjuMAOD5P0S1frXq74H5vwgmyb/w8bQ8cf540/9OTekVk9d4TxbCnliWZcwW6yg9fMjlaNZ5ktU56idO960bkFmvWgXaZV31ZyQT0ZB7gGN0JZmyQD1OYr3vl0S+tYKrFkScGD9tVh6pnPytfXiGREJV86/f13pMR0Y5dsFz/sdW1r3EnTw61rqnb1JPe1e5G1e1gYJBedtV5FCLY8mKEGBfaTjz+xRG/ke2k6aGz~-1~-1~-1; akaau=1625694832~id=c8743b067ffde25dd8a8e2e21ebcedc1; bm_mi=AA1BCFBB266DB50172B246E145E90842~fkYA+VJId/DkSrYKOx7WWpPSU7H5EOC2fxTxaJB3gm+tONq5Mdx5vvlaHE9jwJ+4x1gU0Cbnve/Z/WG02CwH4hu4UeskqtDMpbH/Xbct7q+oRdjkijfB7t2hZCQ+9bOvPqJKKW/Q3LZPfWPE0wK/GQ/H//sxb7mmd5+V0XScRD0TWLe/KOSl1Vdx0FUUFf/4xLN8i3e/HwcNyhCQaGi/ogftNrbBFX7xcqDhPMAsc31ArF9y11Wbrs8F/+olmyvfNhyPm6CkATVrSot2M1XcrE3GB6Oo5nMebqInsaxpjck=; IN_STORE_API_SESSION=TRUE",
            'cache-control': "no-cache",
            'postman-token': "6625e505-7417-088a-9a2d-32043296485c"
        }
    response = make_http_request(url, payload, head, querystring, headers=head, method='POST')
    return response.json()


def get_list_of_products(real_taxonomy):
    """It returns listing pages as a json data.

    :param real_taxonomy: taxonomy with all required data.
    :return: listing_page: listing page, type: json, listing_metadata: metadata of listing, type: list
    """

    listing_metadata = []
    listing_page = []
    real_taxonomy = json.loads(real_taxonomy)['navigation']
    for taxonomy in real_taxonomy:
        required_dep = taxonomy['department_name']
        required_cat = taxonomy['category_name']
        store_id = taxonomy['store_id']
        delivery_zip = taxonomy['delivery_zip']
        required_subcat = ''
        nav_param = taxonomy['nav_param']
        if 'subcategory_name' in taxonomy.keys():
            required_subcat = taxonomy['subcategory_name']
        required_brand = taxonomy['brand_name']
        url = taxonomy['url']
        referer = url
        http_request = make_http_request(url, headers=headers, type_of_result='HTML')
        tree = html.fromstring(http_request.content)
        total_counts = tree.xpath("//span[@class='results-applied__label']//text()")[0]
        page_counts = ceil(int(total_counts) / PRODUCTS_PER_PAGE)
        page_body = tree.xpath("//div[@class='results-wrapped']//div[@class='grid']//section[contains(@id, 'browse-search-pods')]")
        metadata = [store_id, delivery_zip, required_dep, required_cat, required_subcat, required_brand]
        for page in range(page_counts):
            print(f'IN PROGRESS: dep: {required_dep} cat: {required_cat} sub: {required_subcat} brand: {required_brand} page: {page+1}, store_id: {store_id}')
            data_temp = pagination(nav_param, page, store_id, delivery_zip, referer)
            listing_page.append(data_temp)
            listing_metadata.append(metadata)

    return listing_page, listing_metadata


def parse_product_details(listing_with_products):
    """Extracting products from listing and saving to the file as a json file.

    :param listing_with_products: listing page, that contains products and metadata.
    """

    json_result = defaultdict(list)
    result = {}
    listing = listing_with_products[0]
    listing_metadata = listing_with_products[1]
    for idx, listing_p in enumerate(listing):
        store_id, delivery_zip, department_name, category_name, sub_category_name, brand = listing_metadata[idx]
        result['store_id'] = store_id
        result['delivery_zip'] = delivery_zip
        result['department_name'] = department_name
        result['category_name'] = category_name
        result['sub_category_name'] = sub_category_name
        result['brand'] = brand
        products = listing_p['data']['searchModel']['products']
        for product in products:
            product_details = product.get('identifiers', '')
            if product_details != '':
                result['brand'] = product_details.get('brandName', '')
                result['product_url'] = HOME_URL1 + product_details.get('canonicalUrl', '')
                result['item_id'] = product_details.get('itemId', '')
                result['model_number'] = product_details.get('modelNumber', '')
                result['product_name'] = product_details.get('productLabel', '')
                result['store_sku'] = product_details.get('storeSkuNumber', '')
            product_details = product.get('info', '')
            if product_details != '':
                result['breadcrumbs'] = product_details.get('categoryHierarchy', '')
                result['availability'] = '1'
            if 'pricing' in product.keys():
                product_details = product['pricing']
            if product_details is not None:
                if 'original' in product_details.keys():
                    result['price_retail'] = product_details.get('original', '')
                else:
                    result['price_retail'] = 'Price not available.'
                    result['availability'] = '0'
                if 'value' in product_details.keys():
                    result['current_price'] = product_details.get('value', '')
                else:
                    result['current_price'] = 'Price not available.'
                    result['availability'] = '0'
            else:
                result['price_retail'] = 'Price not available.'
                result['current_price'] = 'Price not available.'
                result['availability'] = 'NO'
            product_details = product['reviews']['ratingsReviews']
            result['total_reviews'] = product_details.get('totalReviews', '')
            result['average_rating'] = product_details.get('averageRating', '')
            product_detail_copy = result.copy()
            json_result['products'].append(product_detail_copy)

    with open(f'data.json', 'a+') as outfile:
        json.dump(json_result, outfile)

    print("Data has been scraped!!!")


def group_by():
    """Grouping data with using groupby. Group by counts on sub_category_name level.
    :param: file with product_data
    """

    file = 'data.json'  # File must be within the same directory as this script.
    with open(file) as data_file:
        data_hd = json.load(data_file)
    df = pd.json_normalize(data_hd['products'])
    group_by = df.groupby(["delivery_zip", "store_id", "department_name", "category_name", "sub_category_name", "brand"])["sub_category_name"].count()
    print(group_by)


if __name__ == "__main__":
    full_navigation = get_navigation()
    chosen_navigation = parse_navigation(full_navigation)
    parsed_chosen_navigation = extract_url(chosen_navigation)
    listing_page = get_list_of_products(parsed_chosen_navigation)
    parse_product_details(listing_page)
    group_by()  # To validate if data is consistent with the website.

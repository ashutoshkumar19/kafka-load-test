'use strict';

/**
 * Load producer settings for load-producer.js
 *
 * Kafka brokers/SSL/SASL come from .env via ENV profile (same as monitor).
 * Edit this file to set topic, message count, parallelism, key, and payload.
 *
 * Templates in key/payload/headers: {seq} message number, {ts} ISO timestamp
 */

const path = require('path');

module.exports = {
  /** Target Kafka topic */
  topic: 'Zeus_Rewards_Products_Sync_Private_Perf',

  /** Total messages to produce */
  messageCount: 1000000,

  /** Concurrent in-flight sends (sliding window, not batch-and-wait) */
  parallelism: 100,

  /**
   * If true, queue sends without awaiting each ack (still capped by parallelism).
   * Faster, but errors surface later and disconnect must flush the producer queue.
   */
  fireAndForget: false,

  /** Message key — append -{seq} unless fixedKey is true */
  key: null,

  /** If true, every message uses the same key */
  fixedKey: false,

  /** Message body (string). Use {seq} and {ts} for per-message values. */
  payload: {
    aggregateId: 'ffff5d91-5bbc-4b17-8131-419ffc6d1eaa',
    aggregateType: 'Reward_Product_Variants',
    value: {
      id: 'ffff5d91-5bbc-4b17-8131-419ffc6d1eaa',
      tenantId: null,
      vendorId: 'd13dabe7-0a4f-482c-98b7-b2c4921400e8',
      vendorProductId: '10881296',
      name: 'Newpath Iwb Volcanoes Site License Multimedia Lesson CD',
      description:
        '<p>NewPath Learning CD Multimedia Lesson Volcanoes Interactive Activities include a virtual lab investigation, volcano simulator, lesson presentation and resources. Multimedia lesson for interactive whiteboard that covers life cycle of a volcanoes and plate boundaries. It also covers volcanic eruptions and landforms. Site license offers for unlimited use within one school building.</p>',
      shortDescription: null,
      terms: null,
      disclaimer: null,
      brand: 'NewPath',
      model: '1449729',
      status: 'ACTIVE',
      currency: 'USD',
      countries: ['CA', 'AU', 'GB', 'US'],
      price: '140.78',
      priceType: 'FIXED',
      productSegment: 'REWARD',
      productType: 'MERCHANDISE',
      fulfillmentType: 'PHYSICAL',
      minQuantity: 1,
      maxQuantity: null,
      minPrice: null,
      maxPrice: null,
      fee: null,
      isOutOfStock: false,
      isOnSale: false,
      isWholeAmountRequired: true,
      redemptionInstructions: null,
      attributes: null,
      imageUrls: [
        'https://d12w9lfqeljony.cloudfront.net/testing_environment/c9f0f895fb98ab9159f51fd0297e236d/110x136/a916c1482c8937a399c0942238d563d4131774c7_10881296.png',
      ],
      metadata: {
        Id: '10881296',
        MSRP: '140.78',
        Brand: 'NewPath',
        Model: '1449729',
        'Image Url':
          'https://d12w9lfqeljony.cloudfront.net/testing_environment/c9f0f895fb98ab9159f51fd0297e236d/110x136/a916c1482c8937a399c0942238d563d4131774c7_10881296.png',
        Attributes: '',
        'Catalog Id': '368',
        'Gallery Id': '468',
        'Is On Sale': '0',
        vendorName: 'carlton',
        'Category Id': '919521',
        'Reward Name': 'Newpath Iwb Volcanoes Site License Multimedia Lesson CD',
        'Gallery MSRP': '140.78',
        'Product Cost': '109.61',
        'Handling Cost': '0',
        'Shipping Cost': '9.71',
        'Service Charge': '0',
        'Catalog Currency': 'USD',
        'Gallery Currency': 'USD',
        'Reward Description':
          '<p>NewPath Learning CD Multimedia Lesson Volcanoes Interactive Activities include a virtual lab investigation, volcano simulator, lesson presentation and resources. Multimedia lesson for interactive whiteboard that covers life cycle of a volcanoes and plate boundaries. It also covers volcanic eruptions and landforms. Site license offers for unlimited use within one school building.</p>',
        'Regular Product Cost': '0',
        'Regular Handling Cost': '0',
        'Regular Shipping Cost': '0',
        'Regular Service Charge': '0',
      },
      lastCheckAt: '2026-06-27T18:28:25.544Z',
      createdAt: '2026-05-16T06:14:42.158Z',
      updatedAt: '2026-06-27T18:28:25.545Z',
      createdBy: 'SYSTEM',
      updatedBy: 'SYSTEM',
      variants: [],
      translations: [],
      variantTranslations: [],
      categories: [
        {
          id: 'e0a31104-3d45-4309-a0fd-14fea9506b2c',
          name: 'Learning Toys & Systems',
          description: null,
          status: 'ACTIVE',
          parentId: '3ffa95a9-7abe-4806-af89-9080499f2436',
          vendorCategoryId: '919521',
          metadata: {
            'Catalog Id': '368',
            'Gallery Id': '468',
            vendorName: 'carlton',
            'Category Id': '919521',
            'Parent Name': 'Toys &amp; Games',
            'Category Name': 'Learning Toys &amp; Systems',
            'Parent Category Id': '919402',
          },
          type: 'DEFAULT',
          productMappings: [
            {
              id: '8e0776b7-15a7-47c6-b887-aea8647fd14f',
              productId: 'ffff5d91-5bbc-4b17-8131-419ffc6d1eaa',
              categoryId: 'e0a31104-3d45-4309-a0fd-14fea9506b2c',
            },
          ],
          parentName: 'Toys & Games',
          parentStatus: 'ACTIVE',
          parentType: 'DEFAULT',
        },
      ],
      categoriesTranslations: [],
      trendingCategories: [],
    },
  },

  /**
   * Optional: read payload from a file instead of `payload` above.
   * Path is relative to this file's directory unless absolute.
   */
  payloadFile: null,
  // payloadFile: path.join(__dirname, 'payloads', 'sample.json'),

  /**
   * Kafka message headers (string values). Use {seq} and {ts} for per-message values.
   * Optional headersFile (JSON object) overrides this when set.
   */
  headers: {
    'x-smtip-cid': '90066720-bebd-4d3b-b7ae-f7fe6ba8ef76',
    traceparent: '00-04fd3fb0416288de4dbbb8105e72cca5-511c57d3e51116eb-01',
    source: 'load-test',
  },
  headersFile: null,
  // headersFile: path.join(__dirname, 'payloads', 'headers.json'),

  /** Kafka client id for the producer connection */
  clientId: 'kafka-load-producer',
};

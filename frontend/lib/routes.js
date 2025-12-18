export const ROUTES = {
  // Health & Status
  HEALTH: '/api/health',
  NODES_STATUS: '/api/nodes',

  // Transaction Management
  TRANSFER: '/api/transaction/transfer',
  TRANSACTION_STATUS: (tid) => `/api/transactions/${tid}`,
  TRANSACTIONS_LIST: '/api/transactions',

  // Failure Injection (for demo)
  FAILURE_CRASH: '/api/failure/inject/crash',
  FAILURE_DELAY: '/api/failure/inject/delay',
};
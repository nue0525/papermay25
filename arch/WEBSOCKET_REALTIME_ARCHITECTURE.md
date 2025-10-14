# WebSocket Real-time Architecture

## Overview

The UZO platform uses **WebSocket connections** for real-time updates without page refresh. This enables live workflow execution updates, theme changes, dashboard updates, and collaborative features.

---

## 1. Requirements

### Real-time Update Types

| Update Type | Description | Frequency | Priority |
|------------|-------------|-----------|----------|
| **Workflow Execution** | Node status, progress, completion | Every 100ms | Critical |
| **Job Status** | Job state changes | Event-driven | High |
| **Theme Changes** | User theme updates | On change | Medium |
| **Dashboard Updates** | Metrics, charts refresh | Every 5s | Medium |
| **Notifications** | Alerts, messages | Event-driven | High |
| **Collaborative Editing** | Multiple users editing workflow | Real-time | High |
| **Usage Metrics** | Live resource usage | Every 10s | Low |
| **Feature Flags** | Subscription upgrades | Event-driven | Medium |

### System Requirements

- ✅ **Low latency**: < 50ms message delivery
- ✅ **High throughput**: 10,000+ concurrent connections
- ✅ **Message persistence**: Replay missed messages
- ✅ **Authentication**: JWT-based connection auth
- ✅ **Authorization**: Subscription-level isolation
- ✅ **Scalability**: Horizontal scaling with Redis pub/sub
- ✅ **Reconnection**: Automatic reconnect with backoff
- ✅ **Message ordering**: Guaranteed delivery order

---

## 2. Architecture Overview

### Tech Stack

```
Frontend (Next.js)
    ↓
WebSocket Client (Socket.IO)
    ↓
FastAPI WebSocket Server (Multiple instances)
    ↓
Redis Pub/Sub (Message broker)
    ↓
PostgreSQL (Message persistence)
```

### Why Redis Pub/Sub?

- **Horizontal scaling**: Multiple FastAPI servers share messages
- **Low latency**: In-memory message routing
- **Pub/Sub pattern**: Subscribe to specific channels
- **Persistence**: Optional message persistence with Redis Streams

---

## 3. Backend WebSocket Implementation

### FastAPI WebSocket Manager

```python
# services/websocket_manager.py

from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set, Optional, Any
import asyncio
import json
import logging
from datetime import datetime
from uuid import uuid4
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

class WebSocketManager:
    """
    Manages WebSocket connections with Redis pub/sub for scaling
    """

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

        # Connection registry: {subscription_id: {connection_id: websocket}}
        self.connections: Dict[str, Dict[str, WebSocket]] = {}

        # User to connection mapping: {user_id: [connection_ids]}
        self.user_connections: Dict[str, Set[str]] = {}

        # Channel subscriptions: {channel: {connection_id}}
        self.channel_subscriptions: Dict[str, Set[str]] = {}

        # Redis pubsub instance
        self.pubsub: Optional[aioredis.client.PubSub] = None

        # Background task for Redis subscription
        self.redis_task: Optional[asyncio.Task] = None

    # =========================================================================
    # 1. CONNECTION MANAGEMENT
    # =========================================================================

    async def connect(
        self,
        websocket: WebSocket,
        user_id: str,
        subscription_id: str
    ) -> str:
        """
        Accept WebSocket connection and register it
        Returns: connection_id
        """

        await websocket.accept()

        connection_id = str(uuid4())

        # Register connection
        if subscription_id not in self.connections:
            self.connections[subscription_id] = {}

        self.connections[subscription_id][connection_id] = websocket

        # Track user connections
        if user_id not in self.user_connections:
            self.user_connections[user_id] = set()

        self.user_connections[user_id].add(connection_id)

        logger.info(
            f"WebSocket connected: connection_id={connection_id}, "
            f"user_id={user_id}, subscription_id={subscription_id}"
        )

        # Send connection confirmation
        await self.send_to_connection(
            connection_id,
            subscription_id,
            {
                "type": "connection_established",
                "connectionId": connection_id,
                "userId": user_id,
                "subscriptionId": subscription_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )

        # Start Redis subscription if not already running
        if not self.redis_task:
            self.redis_task = asyncio.create_task(self._redis_subscriber())

        return connection_id

    async def disconnect(
        self,
        connection_id: str,
        subscription_id: str,
        user_id: str
    ):
        """
        Remove connection and clean up
        """

        # Remove from subscription connections
        if subscription_id in self.connections:
            self.connections[subscription_id].pop(connection_id, None)

            if not self.connections[subscription_id]:
                del self.connections[subscription_id]

        # Remove from user connections
        if user_id in self.user_connections:
            self.user_connections[user_id].discard(connection_id)

            if not self.user_connections[user_id]:
                del self.user_connections[user_id]

        # Remove from channel subscriptions
        for channel in list(self.channel_subscriptions.keys()):
            self.channel_subscriptions[channel].discard(connection_id)

            if not self.channel_subscriptions[channel]:
                del self.channel_subscriptions[channel]

        logger.info(f"WebSocket disconnected: connection_id={connection_id}")

    # =========================================================================
    # 2. CHANNEL SUBSCRIPTIONS
    # =========================================================================

    async def subscribe_to_channel(
        self,
        connection_id: str,
        subscription_id: str,
        channel: str
    ):
        """
        Subscribe connection to a specific channel
        Channels: workflow:{id}, job:{id}, user:{id}, subscription:{id}
        """

        if channel not in self.channel_subscriptions:
            self.channel_subscriptions[channel] = set()

        self.channel_subscriptions[channel].add(connection_id)

        # Subscribe to Redis channel
        await self.redis.sadd(f"channel:{channel}:subscribers", connection_id)

        logger.info(f"Connection {connection_id} subscribed to {channel}")

    async def unsubscribe_from_channel(
        self,
        connection_id: str,
        channel: str
    ):
        """
        Unsubscribe connection from channel
        """

        if channel in self.channel_subscriptions:
            self.channel_subscriptions[channel].discard(connection_id)

        await self.redis.srem(f"channel:{channel}:subscribers", connection_id)

        logger.info(f"Connection {connection_id} unsubscribed from {channel}")

    # =========================================================================
    # 3. MESSAGE SENDING
    # =========================================================================

    async def send_to_connection(
        self,
        connection_id: str,
        subscription_id: str,
        message: Dict[str, Any]
    ):
        """
        Send message to specific connection
        """

        if subscription_id not in self.connections:
            return

        websocket = self.connections[subscription_id].get(connection_id)

        if not websocket:
            return

        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send message to {connection_id}: {e}")
            await self.disconnect(connection_id, subscription_id, "")

    async def send_to_user(
        self,
        user_id: str,
        message: Dict[str, Any]
    ):
        """
        Send message to all connections of a user
        """

        if user_id not in self.user_connections:
            return

        connection_ids = list(self.user_connections[user_id])

        for connection_id in connection_ids:
            # Find subscription_id for this connection
            for subscription_id, conns in self.connections.items():
                if connection_id in conns:
                    await self.send_to_connection(
                        connection_id, subscription_id, message
                    )
                    break

    async def send_to_subscription(
        self,
        subscription_id: str,
        message: Dict[str, Any]
    ):
        """
        Send message to all connections in a subscription
        """

        if subscription_id not in self.connections:
            return

        connection_ids = list(self.connections[subscription_id].keys())

        for connection_id in connection_ids:
            await self.send_to_connection(connection_id, subscription_id, message)

    async def broadcast_to_channel(
        self,
        channel: str,
        message: Dict[str, Any]
    ):
        """
        Broadcast message to all subscribers of a channel
        Uses Redis pub/sub for horizontal scaling
        """

        message["channel"] = channel
        message["timestamp"] = datetime.utcnow().isoformat()

        # Publish to Redis (will be received by all FastAPI instances)
        await self.redis.publish(
            f"websocket:{channel}",
            json.dumps(message)
        )

        logger.debug(f"Broadcasted to channel {channel}: {message['type']}")

    # =========================================================================
    # 4. REDIS SUBSCRIBER (Background Task)
    # =========================================================================

    async def _redis_subscriber(self):
        """
        Background task to listen to Redis pub/sub
        Distributes messages to local WebSocket connections
        """

        self.pubsub = self.redis.pubsub()

        # Subscribe to all websocket channels
        await self.pubsub.psubscribe("websocket:*")

        logger.info("Redis subscriber started")

        try:
            async for message in self.pubsub.listen():
                if message["type"] == "pmessage":
                    try:
                        channel = message["channel"].decode("utf-8").replace("websocket:", "")
                        data = json.loads(message["data"])

                        # Send to local connections subscribed to this channel
                        await self._distribute_message(channel, data)

                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}")

        except asyncio.CancelledError:
            logger.info("Redis subscriber stopped")
        finally:
            await self.pubsub.unsubscribe("websocket:*")
            await self.pubsub.close()

    async def _distribute_message(
        self,
        channel: str,
        message: Dict[str, Any]
    ):
        """
        Distribute message to local connections subscribed to channel
        """

        if channel not in self.channel_subscriptions:
            return

        connection_ids = list(self.channel_subscriptions[channel])

        for connection_id in connection_ids:
            # Find subscription_id for this connection
            for subscription_id, conns in self.connections.items():
                if connection_id in conns:
                    await self.send_to_connection(
                        connection_id, subscription_id, message
                    )
                    break

    # =========================================================================
    # 5. WORKFLOW EXECUTION UPDATES
    # =========================================================================

    async def broadcast_workflow_update(
        self,
        workflow_id: str,
        execution_id: str,
        update_type: str,
        data: Dict[str, Any]
    ):
        """
        Broadcast workflow execution update
        """

        message = {
            "type": "workflow_update",
            "updateType": update_type,  # 'started', 'node_started', 'node_completed', 'completed', 'failed'
            "workflowId": workflow_id,
            "executionId": execution_id,
            "data": data
        }

        await self.broadcast_to_channel(f"workflow:{workflow_id}", message)

    async def broadcast_node_update(
        self,
        workflow_id: str,
        execution_id: str,
        node_id: str,
        status: str,
        data: Optional[Dict[str, Any]] = None
    ):
        """
        Broadcast node execution update
        """

        message = {
            "type": "node_update",
            "workflowId": workflow_id,
            "executionId": execution_id,
            "nodeId": node_id,
            "status": status,  # 'running', 'success', 'error'
            "data": data or {}
        }

        await self.broadcast_to_channel(f"workflow:{workflow_id}", message)

    # =========================================================================
    # 6. THEME & DASHBOARD UPDATES
    # =========================================================================

    async def broadcast_theme_update(
        self,
        user_id: str,
        theme: str
    ):
        """
        Broadcast theme change to all user's connections
        """

        message = {
            "type": "theme_updated",
            "userId": user_id,
            "theme": theme
        }

        await self.send_to_user(user_id, message)

    async def broadcast_dashboard_update(
        self,
        subscription_id: str,
        dashboard_data: Dict[str, Any]
    ):
        """
        Broadcast dashboard updates to all subscription users
        """

        message = {
            "type": "dashboard_updated",
            "data": dashboard_data
        }

        await self.send_to_subscription(subscription_id, message)

    # =========================================================================
    # 7. FEATURE FLAG UPDATES
    # =========================================================================

    async def broadcast_subscription_update(
        self,
        subscription_id: str,
        update_type: str,
        data: Dict[str, Any]
    ):
        """
        Broadcast subscription changes (plan upgrade, feature enabled, etc.)
        """

        message = {
            "type": "subscription_updated",
            "updateType": update_type,  # 'plan_upgraded', 'feature_enabled', 'limit_reached'
            "data": data
        }

        await self.send_to_subscription(subscription_id, message)

    # =========================================================================
    # 8. NOTIFICATIONS
    # =========================================================================

    async def send_notification(
        self,
        user_id: str,
        notification: Dict[str, Any]
    ):
        """
        Send notification to user
        """

        message = {
            "type": "notification",
            "notification": notification
        }

        await self.send_to_user(user_id, message)


# =============================================================================
# GLOBAL WEBSOCKET MANAGER INSTANCE
# =============================================================================

websocket_manager: Optional[WebSocketManager] = None

def get_websocket_manager(redis_client: aioredis.Redis) -> WebSocketManager:
    """
    Get or create WebSocket manager singleton
    """
    global websocket_manager

    if websocket_manager is None:
        websocket_manager = WebSocketManager(redis_client)

    return websocket_manager
```

---

## 4. FastAPI WebSocket Endpoint

### WebSocket Route with Authentication

```python
# routes/websocket.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, Depends
from Auth.bearer import verify_jwt_token
from services.websocket_manager import get_websocket_manager
from Connection.redisConn import redis_client
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...),  # JWT token as query param
):
    """
    WebSocket endpoint with JWT authentication

    Connection URL: ws://localhost:8002/ws?token={jwt_token}
    """

    # Verify JWT token
    try:
        user_data = verify_jwt_token(token)
        user_id = user_data["user_id"]
        subscription_id = user_data["subscription_id"]

    except Exception as e:
        logger.error(f"WebSocket auth failed: {e}")
        await websocket.close(code=1008)  # Policy violation
        return

    # Get WebSocket manager
    ws_manager = get_websocket_manager(redis_client)

    # Connect
    connection_id = await ws_manager.connect(
        websocket=websocket,
        user_id=user_id,
        subscription_id=subscription_id
    )

    try:
        # Listen for client messages
        while True:
            data = await websocket.receive_json()

            message_type = data.get("type")

            # Handle different message types
            if message_type == "subscribe":
                # Subscribe to channel
                channel = data.get("channel")
                await ws_manager.subscribe_to_channel(
                    connection_id, subscription_id, channel
                )

                await ws_manager.send_to_connection(
                    connection_id,
                    subscription_id,
                    {
                        "type": "subscribed",
                        "channel": channel
                    }
                )

            elif message_type == "unsubscribe":
                # Unsubscribe from channel
                channel = data.get("channel")
                await ws_manager.unsubscribe_from_channel(connection_id, channel)

                await ws_manager.send_to_connection(
                    connection_id,
                    subscription_id,
                    {
                        "type": "unsubscribed",
                        "channel": channel
                    }
                )

            elif message_type == "ping":
                # Heartbeat
                await ws_manager.send_to_connection(
                    connection_id,
                    subscription_id,
                    {"type": "pong"}
                )

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {connection_id}")

    except Exception as e:
        logger.error(f"WebSocket error: {e}")

    finally:
        # Clean up
        await ws_manager.disconnect(connection_id, subscription_id, user_id)


# =============================================================================
# INTEGRATION WITH PREFECT WORKFLOW ENGINE
# =============================================================================

# In Prefect workflow engine, broadcast updates:

from services.websocket_manager import get_websocket_manager

class PrefectWorkflowEngine:

    async def execute_workflow(
        self,
        workflow_id: str,
        execution_id: str,
        ...
    ):
        ws_manager = get_websocket_manager(redis_client)

        # Broadcast workflow started
        await ws_manager.broadcast_workflow_update(
            workflow_id=workflow_id,
            execution_id=execution_id,
            update_type="started",
            data={"startedAt": datetime.utcnow().isoformat()}
        )

        # Execute nodes
        for node_id in execution_order:
            # Broadcast node started
            await ws_manager.broadcast_node_update(
                workflow_id=workflow_id,
                execution_id=execution_id,
                node_id=node_id,
                status="running",
                data={"startedAt": datetime.utcnow().isoformat()}
            )

            # Execute node
            result = await self.execute_node(node, context)

            # Broadcast node completed
            await ws_manager.broadcast_node_update(
                workflow_id=workflow_id,
                execution_id=execution_id,
                node_id=node_id,
                status="success",
                data={
                    "completedAt": datetime.utcnow().isoformat(),
                    "rowsProcessed": result.get("rows", 0)
                }
            )

        # Broadcast workflow completed
        await ws_manager.broadcast_workflow_update(
            workflow_id=workflow_id,
            execution_id=execution_id,
            update_type="completed",
            data={"completedAt": datetime.utcnow().isoformat()}
        )
```

---

## 5. Frontend WebSocket Client

### React Context for WebSocket

```typescript
// contexts/WebSocketContext.tsx

import { createContext, useContext, useEffect, useState, useRef, ReactNode } from 'react';
import { io, Socket } from 'socket.io-client';
import { useAuth } from './AuthContext';

interface WebSocketMessage {
  type: string;
  [key: string]: any;
}

interface WebSocketContextType {
  isConnected: boolean;
  subscribe: (channel: string) => void;
  unsubscribe: (channel: string) => void;
  on: (eventType: string, callback: (data: any) => void) => void;
  off: (eventType: string, callback: (data: any) => void) => void;
}

const WebSocketContext = createContext<WebSocketContextType | null>(null);

export function WebSocketProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth();
  const [isConnected, setIsConnected] = useState(false);
  const socketRef = useRef<WebSocket | null>(null);
  const eventHandlers = useRef<Map<string, Set<Function>>>(new Map());
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);
  const reconnectAttempts = useRef(0);

  useEffect(() => {
    if (!user?.token) {
      return;
    }

    connectWebSocket();

    return () => {
      disconnectWebSocket();
    };
  }, [user]);

  function connectWebSocket() {
    if (socketRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    const wsUrl = `${process.env.NEXT_PUBLIC_WS_URL}/ws?token=${user.token}`;
    const socket = new WebSocket(wsUrl);

    socket.onopen = () => {
      console.log('WebSocket connected');
      setIsConnected(true);
      reconnectAttempts.current = 0;
    };

    socket.onclose = () => {
      console.log('WebSocket disconnected');
      setIsConnected(false);

      // Reconnect with exponential backoff
      const backoffDelay = Math.min(1000 * Math.pow(2, reconnectAttempts.current), 30000);
      reconnectTimeout.current = setTimeout(() => {
        reconnectAttempts.current++;
        connectWebSocket();
      }, backoffDelay);
    };

    socket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    socket.onmessage = (event) => {
      try {
        const message: WebSocketMessage = JSON.parse(event.data);
        handleMessage(message);
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };

    socketRef.current = socket;
  }

  function disconnectWebSocket() {
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current);
    }

    if (socketRef.current) {
      socketRef.current.close();
      socketRef.current = null;
    }
  }

  function handleMessage(message: WebSocketMessage) {
    const { type } = message;

    const handlers = eventHandlers.current.get(type);
    if (handlers) {
      handlers.forEach(handler => handler(message));
    }
  }

  function subscribe(channel: string) {
    if (!socketRef.current || socketRef.current.readyState !== WebSocket.OPEN) {
      console.error('WebSocket not connected');
      return;
    }

    socketRef.current.send(JSON.stringify({
      type: 'subscribe',
      channel
    }));
  }

  function unsubscribe(channel: string) {
    if (!socketRef.current || socketRef.current.readyState !== WebSocket.OPEN) {
      return;
    }

    socketRef.current.send(JSON.stringify({
      type: 'unsubscribe',
      channel
    }));
  }

  function on(eventType: string, callback: (data: any) => void) {
    if (!eventHandlers.current.has(eventType)) {
      eventHandlers.current.set(eventType, new Set());
    }

    eventHandlers.current.get(eventType)!.add(callback);
  }

  function off(eventType: string, callback: (data: any) => void) {
    const handlers = eventHandlers.current.get(eventType);
    if (handlers) {
      handlers.delete(callback);
    }
  }

  return (
    <WebSocketContext.Provider value={{ isConnected, subscribe, unsubscribe, on, off }}>
      {children}
    </WebSocketContext.Provider>
  );
}

export function useWebSocket() {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
}
```

### React Hook for Workflow Updates

```typescript
// hooks/useWorkflowUpdates.ts

import { useEffect, useState } from 'react';
import { useWebSocket } from '@/contexts/WebSocketContext';

interface NodeUpdate {
  nodeId: string;
  status: 'running' | 'success' | 'error';
  data: any;
}

interface WorkflowUpdates {
  status: 'idle' | 'running' | 'completed' | 'failed';
  nodeUpdates: Record<string, NodeUpdate>;
  executionId: string | null;
}

export function useWorkflowUpdates(workflowId: string) {
  const { subscribe, unsubscribe, on, off } = useWebSocket();
  const [updates, setUpdates] = useState<WorkflowUpdates>({
    status: 'idle',
    nodeUpdates: {},
    executionId: null
  });

  useEffect(() => {
    // Subscribe to workflow channel
    const channel = `workflow:${workflowId}`;
    subscribe(channel);

    // Handle workflow updates
    const handleWorkflowUpdate = (message: any) => {
      if (message.workflowId !== workflowId) return;

      setUpdates(prev => ({
        ...prev,
        status: message.updateType === 'started' ? 'running' :
                message.updateType === 'completed' ? 'completed' :
                message.updateType === 'failed' ? 'failed' : prev.status,
        executionId: message.executionId
      }));
    };

    // Handle node updates
    const handleNodeUpdate = (message: any) => {
      if (message.workflowId !== workflowId) return;

      setUpdates(prev => ({
        ...prev,
        nodeUpdates: {
          ...prev.nodeUpdates,
          [message.nodeId]: {
            nodeId: message.nodeId,
            status: message.status,
            data: message.data
          }
        }
      }));
    };

    on('workflow_update', handleWorkflowUpdate);
    on('node_update', handleNodeUpdate);

    return () => {
      off('workflow_update', handleWorkflowUpdate);
      off('node_update', handleNodeUpdate);
      unsubscribe(channel);
    };
  }, [workflowId]);

  return updates;
}

// Usage in component:

export function WorkflowCanvas({ workflowId }: { workflowId: string }) {
  const { nodeUpdates, status } = useWorkflowUpdates(workflowId);

  // Update ReactFlow nodes with real-time status
  const [nodes, setNodes] = useState<Node[]>([]);

  useEffect(() => {
    setNodes(prevNodes =>
      prevNodes.map(node => {
        const update = nodeUpdates[node.id];
        if (update) {
          return {
            ...node,
            data: {
              ...node.data,
              status: update.status,
              ...update.data
            }
          };
        }
        return node;
      })
    );
  }, [nodeUpdates]);

  return (
    <div>
      <div>Workflow Status: {status}</div>
      <ReactFlow nodes={nodes} edges={edges} />
    </div>
  );
}
```

### Theme Update Hook

```typescript
// hooks/useThemeSync.ts

import { useEffect } from 'react';
import { useWebSocket } from '@/contexts/WebSocketContext';
import { useTheme } from 'next-themes';

export function useThemeSync(userId: string) {
  const { on, off } = useWebSocket();
  const { setTheme } = useTheme();

  useEffect(() => {
    const handleThemeUpdate = (message: any) => {
      if (message.userId === userId) {
        setTheme(message.theme);
      }
    };

    on('theme_updated', handleThemeUpdate);

    return () => {
      off('theme_updated', handleThemeUpdate);
    };
  }, [userId]);
}

// Usage in layout:

export function RootLayout({ children }: { children: ReactNode }) {
  const { user } = useAuth();
  useThemeSync(user?.id);

  return (
    <WebSocketProvider>
      <ThemeProvider>
        {children}
      </ThemeProvider>
    </WebSocketProvider>
  );
}
```

---

## 6. Horizontal Scaling

### Multiple FastAPI Instances with Redis Pub/Sub

```
                    Load Balancer
                          |
        +----------------+----------------+
        |                                 |
   FastAPI #1                        FastAPI #2
   (ws_manager_1)                    (ws_manager_2)
        |                                 |
        +----------------+----------------+
                         |
                    Redis Pub/Sub
```

### How it works:

1. **Client connects** to FastAPI #1 via load balancer
2. **FastAPI #1** accepts WebSocket and subscribes to Redis channels
3. **Prefect workflow** executes on any worker
4. **Worker publishes** update to Redis: `PUBLISH websocket:workflow:123 {...}`
5. **Both FastAPI instances** receive the message
6. **Each instance** sends to its local WebSocket connections
7. **Client receives** update in real-time

---

## 7. Message Persistence (Optional)

### Redis Streams for Message Replay

```python
# Store messages in Redis Stream for replay

async def persist_message(
    channel: str,
    message: Dict[str, Any]
):
    """
    Store message in Redis Stream for replay on reconnect
    """

    await redis_client.xadd(
        f"stream:{channel}",
        message,
        maxlen=1000  # Keep last 1000 messages
    )

# Replay missed messages on reconnect

async def replay_missed_messages(
    connection_id: str,
    channel: str,
    last_message_id: str
):
    """
    Replay messages since last_message_id
    """

    messages = await redis_client.xread(
        {f"stream:{channel}": last_message_id},
        count=100
    )

    for stream, message_list in messages:
        for message_id, message_data in message_list:
            await ws_manager.send_to_connection(
                connection_id,
                subscription_id,
                message_data
            )
```

---

## 8. Performance Optimization

### Connection Pooling & Rate Limiting

```python
# Rate limiting per connection

from datetime import datetime, timedelta

class RateLimiter:
    def __init__(self, max_messages: int = 100, window_seconds: int = 60):
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.message_counts: Dict[str, List[datetime]] = {}

    async def check_limit(self, connection_id: str) -> bool:
        """
        Check if connection exceeded rate limit
        """

        now = datetime.utcnow()
        window_start = now - timedelta(seconds=self.window_seconds)

        if connection_id not in self.message_counts:
            self.message_counts[connection_id] = []

        # Remove old messages outside window
        self.message_counts[connection_id] = [
            ts for ts in self.message_counts[connection_id]
            if ts > window_start
        ]

        # Check limit
        if len(self.message_counts[connection_id]) >= self.max_messages:
            return False  # Rate limit exceeded

        # Add current message
        self.message_counts[connection_id].append(now)
        return True
```

---

## 9. Summary

### WebSocket System Features

| Feature | Implementation | Performance |
|---------|---------------|-------------|
| **Real-time updates** | WebSocket + Redis pub/sub | < 50ms latency |
| **Horizontal scaling** | Redis pub/sub broker | Unlimited instances |
| **Authentication** | JWT token in query param | Per-connection |
| **Channel subscriptions** | Topic-based routing | O(1) lookup |
| **Message persistence** | Redis Streams (optional) | 1000 messages |
| **Reconnection** | Exponential backoff | Automatic |
| **Rate limiting** | Sliding window | 100 msg/min |

### Message Types

| Type | Description | Frequency |
|------|-------------|-----------|
| `workflow_update` | Workflow status change | Per workflow event |
| `node_update` | Node execution status | Every 100ms |
| `theme_updated` | User theme change | On change |
| `dashboard_updated` | Dashboard metrics | Every 5s |
| `subscription_updated` | Plan/feature change | On change |
| `notification` | Alert/message | Event-driven |

### Integration Points

1. **Prefect Workflow Engine**: Broadcast execution updates
2. **Feature Flag Service**: Broadcast subscription changes
3. **User Settings**: Broadcast theme/preference changes
4. **Monitoring Service**: Broadcast dashboard metrics
5. **Notification Service**: Send real-time alerts

### Next Steps

- Implement message compression (gzip)
- Add WebSocket health checks
- Create admin dashboard for connection monitoring
- Implement collaborative workflow editing
- Add presence indicators (who's viewing workflow)

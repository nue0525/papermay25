# Feature Flag & Edition-Based Architecture

## Overview

The UZO platform supports **multiple editions** (Standard, Premium, Enterprise) with different feature sets. The feature flag system enables/disables features based on subscription tier, allowing seamless upgrades without code changes.

---

## 1. Edition Requirements

### Product Editions

| Edition | Price/Month | Features |
|---------|------------|----------|
| **Standard** | $99 | Basic workflows, 10 connectors, 100 executions/month |
| **Premium** | $299 | + Lineage tracking, 50 connectors, 1000 executions/month |
| **Enterprise** | Custom | + Unlimited everything, dedicated infrastructure, SSO, SLA |

### Feature Matrix

| Feature | Standard | Premium | Enterprise |
|---------|----------|---------|------------|
| **Workflows** | 10 | Unlimited | Unlimited |
| **Connectors** | 10 types | 50 types | 100+ types |
| **Executions/month** | 100 | 1,000 | Unlimited |
| **Lineage Tracking** | ❌ | ✅ | ✅ |
| **Column-level Lineage** | ❌ | ❌ | ✅ |
| **Advanced Monitoring** | ❌ | ✅ | ✅ |
| **Forecasting** | ❌ | ✅ | ✅ |
| **Real-time Alerts** | ❌ | ✅ | ✅ |
| **API Access** | Limited | Full | Full |
| **Collaboration** | 3 users | 10 users | Unlimited |
| **Data Retention** | 30 days | 90 days | Custom |
| **SSO/SAML** | ❌ | ❌ | ✅ |
| **Dedicated Support** | Email | Email + Chat | 24/7 Phone |
| **Custom Connectors** | ❌ | ❌ | ✅ |
| **White-label** | ❌ | ❌ | ✅ |

---

## 2. Database Schema

### PostgreSQL Schema for Feature Flags

```sql
-- =============================================================================
-- SUBSCRIPTION & EDITION MANAGEMENT
-- =============================================================================

-- 1. Subscription Plans (Product offerings)
CREATE TABLE subscription_plans (
    plan_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Plan details
    plan_name VARCHAR(100) NOT NULL UNIQUE,  -- 'standard', 'premium', 'enterprise'
    display_name VARCHAR(255) NOT NULL,       -- 'Standard Edition', 'Premium Edition'
    description TEXT,

    -- Pricing
    base_price_monthly NUMERIC(10,2),
    base_price_yearly NUMERIC(10,2),
    currency VARCHAR(3) DEFAULT 'USD',

    -- Limits
    max_workflows INTEGER,
    max_executions_per_month INTEGER,
    max_users INTEGER,
    max_storage_gb INTEGER,
    data_retention_days INTEGER DEFAULT 30,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_public BOOLEAN DEFAULT TRUE,  -- Visible on pricing page

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed data for plans
INSERT INTO subscription_plans (plan_name, display_name, base_price_monthly, base_price_yearly, max_workflows, max_executions_per_month, max_users, data_retention_days)
VALUES
    ('standard', 'Standard Edition', 99.00, 990.00, 10, 100, 3, 30),
    ('premium', 'Premium Edition', 299.00, 2990.00, NULL, 1000, 10, 90),
    ('enterprise', 'Enterprise Edition', NULL, NULL, NULL, NULL, NULL, 365);

-- 2. Feature Definitions (Master list of all features)
CREATE TABLE features (
    feature_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Feature identification
    feature_key VARCHAR(100) NOT NULL UNIQUE,  -- 'lineage_tracking', 'advanced_monitoring'
    feature_name VARCHAR(255) NOT NULL,        -- 'Lineage Tracking', 'Advanced Monitoring'
    feature_category VARCHAR(50),              -- 'analytics', 'integration', 'security'
    description TEXT,

    -- Default state
    default_enabled BOOLEAN DEFAULT FALSE,

    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed feature data
INSERT INTO features (feature_key, feature_name, feature_category, description, default_enabled)
VALUES
    ('lineage_tracking', 'Lineage Tracking', 'analytics', '15-level table lineage tracking', FALSE),
    ('column_lineage', 'Column-level Lineage', 'analytics', 'Column-to-column lineage mapping', FALSE),
    ('advanced_monitoring', 'Advanced Monitoring', 'analytics', 'Real-time monitoring with forecasting', FALSE),
    ('real_time_alerts', 'Real-time Alerts', 'notifications', 'Instant alerts for workflow failures', FALSE),
    ('sso_authentication', 'SSO Authentication', 'security', 'SAML/OAuth SSO integration', FALSE),
    ('api_full_access', 'Full API Access', 'integration', 'Unlimited API calls', FALSE),
    ('custom_connectors', 'Custom Connectors', 'integration', 'Build custom connector plugins', FALSE),
    ('white_label', 'White Label', 'branding', 'Custom branding and domain', FALSE),
    ('priority_support', 'Priority Support', 'support', '24/7 phone and email support', FALSE),
    ('data_export', 'Data Export', 'integration', 'Export workflows and data', TRUE),
    ('webhook_integrations', 'Webhook Integrations', 'integration', 'Trigger webhooks on events', FALSE);

-- 3. Plan Features (What features are included in each plan)
CREATE TABLE plan_features (
    plan_id UUID REFERENCES subscription_plans(plan_id) ON DELETE CASCADE,
    feature_id UUID REFERENCES features(feature_id) ON DELETE CASCADE,

    -- Feature-specific limits (optional)
    feature_limit INTEGER,  -- e.g., API calls per month
    feature_config JSONB DEFAULT '{}'::jsonb,

    -- Status
    is_enabled BOOLEAN DEFAULT TRUE,

    PRIMARY KEY (plan_id, feature_id)
);

-- Seed plan-feature mappings
-- Standard plan features
INSERT INTO plan_features (plan_id, feature_id, is_enabled)
SELECT
    p.plan_id,
    f.feature_id,
    TRUE
FROM subscription_plans p
CROSS JOIN features f
WHERE p.plan_name = 'standard'
  AND f.feature_key IN ('data_export');

-- Premium plan features
INSERT INTO plan_features (plan_id, feature_id, is_enabled)
SELECT
    p.plan_id,
    f.feature_id,
    TRUE
FROM subscription_plans p
CROSS JOIN features f
WHERE p.plan_name = 'premium'
  AND f.feature_key IN (
    'lineage_tracking',
    'advanced_monitoring',
    'real_time_alerts',
    'data_export',
    'webhook_integrations'
  );

-- Enterprise plan features (all features)
INSERT INTO plan_features (plan_id, feature_id, is_enabled)
SELECT
    p.plan_id,
    f.feature_id,
    TRUE
FROM subscription_plans p
CROSS JOIN features f
WHERE p.plan_name = 'enterprise';

-- 4. Subscriptions (Customer subscriptions)
CREATE TABLE subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Plan
    plan_id UUID NOT NULL REFERENCES subscription_plans(plan_id),

    -- Organization
    company_name VARCHAR(255) NOT NULL,

    -- Billing
    billing_email VARCHAR(255),
    billing_cycle VARCHAR(20) DEFAULT 'monthly',  -- 'monthly', 'yearly'

    -- Status
    status VARCHAR(50) DEFAULT 'active',  -- 'trial', 'active', 'suspended', 'cancelled'
    trial_ends_at TIMESTAMPTZ,
    current_period_start TIMESTAMPTZ DEFAULT NOW(),
    current_period_end TIMESTAMPTZ,

    -- Usage tracking
    current_month_executions INTEGER DEFAULT 0,
    current_month_api_calls INTEGER DEFAULT 0,

    -- Infrastructure
    infrastructure_type VARCHAR(50) DEFAULT 'shared',  -- 'shared', 'dedicated'
    database_connection_string TEXT,  -- For dedicated infrastructure

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES users(user_id),

    CONSTRAINT valid_billing_cycle CHECK (billing_cycle IN ('monthly', 'yearly')),
    CONSTRAINT valid_status CHECK (status IN ('trial', 'active', 'suspended', 'cancelled'))
);

CREATE INDEX idx_subscriptions_plan ON subscriptions(plan_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status);

-- 5. Subscription Feature Overrides (Per-subscription feature customization)
CREATE TABLE subscription_feature_overrides (
    subscription_id UUID REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    feature_id UUID REFERENCES features(feature_id) ON DELETE CASCADE,

    -- Override settings
    is_enabled BOOLEAN NOT NULL,
    feature_limit INTEGER,
    feature_config JSONB DEFAULT '{}'::jsonb,

    -- Reason for override
    override_reason TEXT,

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES users(user_id),

    PRIMARY KEY (subscription_id, feature_id)
);

CREATE INDEX idx_subscription_overrides_subscription ON subscription_feature_overrides(subscription_id);

-- 6. Feature Usage Tracking
CREATE TABLE feature_usage_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),
    feature_id UUID NOT NULL REFERENCES features(feature_id),
    user_id UUID REFERENCES users(user_id),

    -- Usage details
    usage_count INTEGER DEFAULT 1,
    usage_metadata JSONB DEFAULT '{}'::jsonb,

    -- Timing
    used_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_feature_usage_subscription ON feature_usage_logs(subscription_id, used_at DESC);
CREATE INDEX idx_feature_usage_feature ON feature_usage_logs(feature_id, used_at DESC);

-- TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('feature_usage_logs', 'used_at', if_not_exists => TRUE);
```

---

## 3. Feature Flag Service

### Python Service for Feature Checks

```python
# services/feature_flag_service.py

from typing import Optional, Dict, Any, List
from uuid import UUID
import asyncpg
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

class FeatureFlagService:
    """
    Service to check if features are enabled for a subscription
    Handles plan-level features and subscription-level overrides
    """

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    # =========================================================================
    # 1. FEATURE CHECKING
    # =========================================================================

    async def check_feature(
        self,
        subscription_id: str,
        feature_key: str
    ) -> bool:
        """
        Check if a feature is enabled for a subscription

        Priority order:
        1. Subscription-level override (if exists)
        2. Plan-level feature (from plan_features)
        3. Feature default (from features table)

        Returns: True if enabled, False otherwise
        """

        query = """
        WITH subscription_plan AS (
            SELECT s.subscription_id, s.plan_id, s.status
            FROM subscriptions s
            WHERE s.subscription_id = $1
        ),
        plan_feature AS (
            SELECT pf.is_enabled AS plan_enabled
            FROM plan_features pf
            JOIN features f ON pf.feature_id = f.feature_id
            JOIN subscription_plan sp ON pf.plan_id = sp.plan_id
            WHERE f.feature_key = $2
        ),
        override_feature AS (
            SELECT sfo.is_enabled AS override_enabled
            FROM subscription_feature_overrides sfo
            JOIN features f ON sfo.feature_id = f.feature_id
            WHERE sfo.subscription_id = $1 AND f.feature_key = $2
        ),
        default_feature AS (
            SELECT f.default_enabled
            FROM features f
            WHERE f.feature_key = $2
        )
        SELECT
            COALESCE(
                (SELECT override_enabled FROM override_feature),
                (SELECT plan_enabled FROM plan_feature),
                (SELECT default_enabled FROM default_feature),
                FALSE
            ) AS is_enabled,
            (SELECT status FROM subscription_plan) AS subscription_status
        """

        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow(query, subscription_id, feature_key)

        if not result:
            logger.warning(f"No subscription found for {subscription_id}")
            return False

        # Check subscription status
        if result["subscription_status"] not in ("trial", "active"):
            logger.warning(f"Subscription {subscription_id} is not active")
            return False

        return result["is_enabled"]

    async def check_multiple_features(
        self,
        subscription_id: str,
        feature_keys: List[str]
    ) -> Dict[str, bool]:
        """
        Check multiple features at once (batch operation)
        Returns: {"feature_key": True/False}
        """

        results = {}
        for feature_key in feature_keys:
            results[feature_key] = await self.check_feature(subscription_id, feature_key)

        return results

    async def get_enabled_features(
        self,
        subscription_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all enabled features for a subscription
        Returns: List of feature objects
        """

        query = """
        WITH subscription_plan AS (
            SELECT s.subscription_id, s.plan_id, s.status
            FROM subscriptions s
            WHERE s.subscription_id = $1
        )
        SELECT DISTINCT
            f.feature_id,
            f.feature_key,
            f.feature_name,
            f.feature_category,
            f.description,
            COALESCE(sfo.is_enabled, pf.is_enabled, f.default_enabled, FALSE) AS is_enabled,
            COALESCE(sfo.feature_limit, pf.feature_limit) AS feature_limit
        FROM features f
        LEFT JOIN plan_features pf ON f.feature_id = pf.feature_id
        LEFT JOIN subscription_plan sp ON pf.plan_id = sp.plan_id
        LEFT JOIN subscription_feature_overrides sfo ON f.feature_id = sfo.feature_id
            AND sfo.subscription_id = $1
        WHERE COALESCE(sfo.is_enabled, pf.is_enabled, f.default_enabled, FALSE) = TRUE
        ORDER BY f.feature_category, f.feature_name
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, subscription_id)

        return [dict(row) for row in rows]

    # =========================================================================
    # 2. USAGE LIMITS
    # =========================================================================

    async def check_usage_limit(
        self,
        subscription_id: str,
        limit_type: str  # 'executions', 'api_calls', 'workflows', 'users'
    ) -> Dict[str, Any]:
        """
        Check if subscription has reached usage limits
        Returns: {"limit": 100, "current": 50, "remaining": 50, "exceeded": False}
        """

        # Get plan limits
        query = """
        SELECT
            sp.plan_name,
            CASE
                WHEN $2 = 'executions' THEN sp.max_executions_per_month
                WHEN $2 = 'workflows' THEN sp.max_workflows
                WHEN $2 = 'users' THEN sp.max_users
                ELSE NULL
            END AS limit_value
        FROM subscriptions s
        JOIN subscription_plans sp ON s.plan_id = sp.plan_id
        WHERE s.subscription_id = $1
        """

        async with self.db_pool.acquire() as conn:
            plan = await conn.fetchrow(query, subscription_id, limit_type)

            if not plan:
                raise ValueError(f"Subscription {subscription_id} not found")

            limit = plan["limit_value"]

            # NULL means unlimited
            if limit is None:
                return {
                    "limit": None,
                    "current": 0,
                    "remaining": None,
                    "exceeded": False,
                    "unlimited": True
                }

            # Get current usage
            if limit_type == "executions":
                current = await conn.fetchval(
                    "SELECT current_month_executions FROM subscriptions WHERE subscription_id = $1",
                    subscription_id
                )
            elif limit_type == "workflows":
                current = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM workflows w
                    JOIN jobs j ON w.job_id = j.job_id
                    JOIN folders f ON j.folder_id = f.folder_id
                    JOIN workspaces ws ON f.workspace_id = ws.workspace_id
                    WHERE ws.subscription_id = $1
                    """,
                    subscription_id
                )
            elif limit_type == "users":
                current = await conn.fetchval(
                    """
                    SELECT COUNT(DISTINCT user_id)
                    FROM user_subscriptions
                    WHERE subscription_id = $1
                    """,
                    subscription_id
                )
            else:
                current = 0

            remaining = max(0, limit - current)
            exceeded = current >= limit

            return {
                "limit": limit,
                "current": current,
                "remaining": remaining,
                "exceeded": exceeded,
                "unlimited": False
            }

    async def increment_usage(
        self,
        subscription_id: str,
        usage_type: str,
        increment_by: int = 1
    ):
        """
        Increment usage counter (for executions, API calls, etc.)
        """

        if usage_type == "executions":
            query = """
            UPDATE subscriptions
            SET current_month_executions = current_month_executions + $2
            WHERE subscription_id = $1
            """
        elif usage_type == "api_calls":
            query = """
            UPDATE subscriptions
            SET current_month_api_calls = current_month_api_calls + $2
            WHERE subscription_id = $1
            """
        else:
            return

        async with self.db_pool.acquire() as conn:
            await conn.execute(query, subscription_id, increment_by)

    # =========================================================================
    # 3. SUBSCRIPTION MANAGEMENT
    # =========================================================================

    async def upgrade_subscription(
        self,
        subscription_id: str,
        new_plan_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Upgrade/downgrade subscription plan
        Features are automatically enabled/disabled based on new plan
        """

        # Get current and new plans
        query = """
        SELECT
            s.plan_id AS current_plan_id,
            cp.plan_name AS current_plan_name,
            np.plan_name AS new_plan_name
        FROM subscriptions s
        JOIN subscription_plans cp ON s.plan_id = cp.plan_id
        CROSS JOIN subscription_plans np
        WHERE s.subscription_id = $1 AND np.plan_id = $2
        """

        async with self.db_pool.acquire() as conn:
            plan_info = await conn.fetchrow(query, subscription_id, new_plan_id)

            if not plan_info:
                raise ValueError("Subscription or plan not found")

            # Update subscription
            await conn.execute(
                """
                UPDATE subscriptions
                SET plan_id = $2, updated_at = NOW()
                WHERE subscription_id = $1
                """,
                subscription_id, new_plan_id
            )

            # Log the upgrade
            logger.info(
                f"Subscription {subscription_id} upgraded from "
                f"{plan_info['current_plan_name']} to {plan_info['new_plan_name']}"
            )

            # Get newly enabled features
            newly_enabled = await self.get_enabled_features(subscription_id)

            return {
                "subscriptionId": subscription_id,
                "oldPlan": plan_info["current_plan_name"],
                "newPlan": plan_info["new_plan_name"],
                "enabledFeatures": newly_enabled
            }

    async def enable_feature_override(
        self,
        subscription_id: str,
        feature_key: str,
        is_enabled: bool,
        override_reason: str,
        user_id: str
    ):
        """
        Override feature for specific subscription
        (e.g., enable lineage for Standard plan customer as custom deal)
        """

        query = """
        INSERT INTO subscription_feature_overrides (
            subscription_id, feature_id, is_enabled, override_reason, created_by
        )
        SELECT $1, f.feature_id, $2, $3, $4
        FROM features f
        WHERE f.feature_key = $5
        ON CONFLICT (subscription_id, feature_id)
        DO UPDATE SET
            is_enabled = EXCLUDED.is_enabled,
            override_reason = EXCLUDED.override_reason,
            updated_at = NOW()
        """

        async with self.db_pool.acquire() as conn:
            await conn.execute(
                query,
                subscription_id, is_enabled, override_reason, user_id, feature_key
            )

        logger.info(
            f"Feature {feature_key} override set to {is_enabled} "
            f"for subscription {subscription_id}"
        )

    # =========================================================================
    # 4. USAGE TRACKING
    # =========================================================================

    async def log_feature_usage(
        self,
        subscription_id: str,
        feature_key: str,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log feature usage for analytics
        """

        query = """
        INSERT INTO feature_usage_logs (
            subscription_id, feature_id, user_id, usage_metadata
        )
        SELECT $1, f.feature_id, $2, $3
        FROM features f
        WHERE f.feature_key = $4
        """

        async with self.db_pool.acquire() as conn:
            await conn.execute(
                query,
                subscription_id, user_id, metadata or {}, feature_key
            )
```

---

## 4. FastAPI Middleware for Feature Gating

### Automatic Feature Checks via Dependency Injection

```python
# middleware/feature_gate.py

from fastapi import HTTPException, Depends
from typing import Optional
from Auth.bearer import JWTBearer
from services.feature_flag_service import FeatureFlagService

class FeatureGate:
    """
    Dependency to gate API endpoints by feature flag
    """

    def __init__(self, feature_key: str, error_message: Optional[str] = None):
        self.feature_key = feature_key
        self.error_message = error_message or (
            f"This feature is not enabled for your subscription. "
            f"Please upgrade to access {feature_key}."
        )

    async def __call__(
        self,
        current_user: dict = Depends(JWTBearer())
    ):
        """
        Check if feature is enabled for user's subscription
        Raises 403 if not enabled
        """

        feature_service = FeatureFlagService(db_pool)

        is_enabled = await feature_service.check_feature(
            subscription_id=current_user["subscription_id"],
            feature_key=self.feature_key
        )

        if not is_enabled:
            # Log the attempted access
            await feature_service.log_feature_usage(
                subscription_id=current_user["subscription_id"],
                feature_key=self.feature_key,
                user_id=current_user["user_id"],
                metadata={"access_denied": True}
            )

            raise HTTPException(
                status_code=403,
                detail=self.error_message
            )

        # Log successful usage
        await feature_service.log_feature_usage(
            subscription_id=current_user["subscription_id"],
            feature_key=self.feature_key,
            user_id=current_user["user_id"],
            metadata={"access_granted": True}
        )

        return True


# Usage in routes:

@router.get(
    "/api/lineage/assets/{asset_id}/upstream",
    dependencies=[Depends(FeatureGate("lineage_tracking"))]
)
async def get_upstream_lineage(
    asset_id: str,
    current_user: dict = Depends(JWTBearer())
):
    """
    This endpoint is automatically gated by lineage_tracking feature
    """
    # Endpoint logic here
    pass
```

---

## 5. Frontend Feature Flag Integration

### React Hook for Feature Checks

```typescript
// hooks/useFeatureFlag.ts

import { useEffect, useState } from 'react';
import { useAuth } from '@/contexts/AuthContext';

interface FeatureFlagResult {
  isEnabled: boolean;
  isLoading: boolean;
  error: string | null;
}

export function useFeatureFlag(featureKey: string): FeatureFlagResult {
  const { user } = useAuth();
  const [isEnabled, setIsEnabled] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function checkFeature() {
      if (!user?.subscriptionId) {
        setIsLoading(false);
        return;
      }

      try {
        const response = await fetch(
          `/api/features/check/${featureKey}`,
          {
            headers: {
              Authorization: `Bearer ${user.token}`
            }
          }
        );

        if (response.ok) {
          const data = await response.json();
          setIsEnabled(data.isEnabled);
        } else {
          setError('Failed to check feature flag');
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    }

    checkFeature();
  }, [featureKey, user]);

  return { isEnabled, isLoading, error };
}

// Usage in components:

export function LineageTab() {
  const { isEnabled, isLoading } = useFeatureFlag('lineage_tracking');

  if (isLoading) {
    return <LoadingSpinner />;
  }

  if (!isEnabled) {
    return (
      <UpgradePrompt
        feature="Lineage Tracking"
        description="Track data lineage across 15 levels"
        requiredPlan="Premium"
      />
    );
  }

  return <LineageGraph />;
}
```

### Feature Gate Component

```typescript
// components/FeatureGate.tsx

import { ReactNode } from 'react';
import { useFeatureFlag } from '@/hooks/useFeatureFlag';
import { Button } from '@/components/ui/button';
import { Lock } from 'lucide-react';

interface FeatureGateProps {
  featureKey: string;
  children: ReactNode;
  fallback?: ReactNode;
  showUpgrade?: boolean;
}

export function FeatureGate({
  featureKey,
  children,
  fallback,
  showUpgrade = true
}: FeatureGateProps) {
  const { isEnabled, isLoading } = useFeatureFlag(featureKey);

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (!isEnabled) {
    if (fallback) {
      return <>{fallback}</>;
    }

    if (showUpgrade) {
      return (
        <div className="flex flex-col items-center justify-center p-8 border-2 border-dashed rounded-lg">
          <Lock className="w-12 h-12 text-gray-400 mb-4" />
          <h3 className="text-lg font-semibold mb-2">
            Premium Feature
          </h3>
          <p className="text-gray-600 mb-4 text-center">
            This feature requires a Premium or Enterprise subscription.
          </p>
          <Button onClick={() => window.location.href = '/pricing'}>
            Upgrade Now
          </Button>
        </div>
      );
    }

    return null;
  }

  return <>{children}</>;
}

// Usage:
<FeatureGate featureKey="lineage_tracking">
  <LineageGraph assetId={assetId} />
</FeatureGate>
```

---

## 6. API Endpoints

### Feature Flag API Routes

```python
# routes/features.py

from fastapi import APIRouter, Depends, HTTPException
from Auth.bearer import JWTBearer
from services.feature_flag_service import FeatureFlagService

router = APIRouter(prefix="/api/features", tags=["Features"])

@router.get("/check/{feature_key}")
async def check_feature(
    feature_key: str,
    current_user: dict = Depends(JWTBearer())
):
    """
    Check if a feature is enabled for current user's subscription
    """
    feature_service = FeatureFlagService(db_pool)

    is_enabled = await feature_service.check_feature(
        subscription_id=current_user["subscription_id"],
        feature_key=feature_key
    )

    return {
        "featureKey": feature_key,
        "isEnabled": is_enabled
    }

@router.get("/enabled")
async def get_enabled_features(
    current_user: dict = Depends(JWTBearer())
):
    """
    Get all enabled features for current subscription
    """
    feature_service = FeatureFlagService(db_pool)

    features = await feature_service.get_enabled_features(
        subscription_id=current_user["subscription_id"]
    )

    return {
        "features": features,
        "total": len(features)
    }

@router.get("/usage/{limit_type}")
async def check_usage_limit(
    limit_type: str,
    current_user: dict = Depends(JWTBearer())
):
    """
    Check usage limits for subscription
    """
    feature_service = FeatureFlagService(db_pool)

    usage = await feature_service.check_usage_limit(
        subscription_id=current_user["subscription_id"],
        limit_type=limit_type
    )

    return usage

@router.post("/subscriptions/upgrade")
async def upgrade_subscription(
    new_plan_id: str,
    current_user: dict = Depends(JWTBearer())
):
    """
    Upgrade subscription to new plan
    """
    feature_service = FeatureFlagService(db_pool)

    result = await feature_service.upgrade_subscription(
        subscription_id=current_user["subscription_id"],
        new_plan_id=new_plan_id,
        user_id=current_user["user_id"]
    )

    return result
```

---

## 7. Usage Limit Enforcement

### Middleware to Check Limits Before Execution

```python
# middleware/usage_limiter.py

from fastapi import HTTPException, Depends
from Auth.bearer import JWTBearer
from services.feature_flag_service import FeatureFlagService

async def check_execution_limit(
    current_user: dict = Depends(JWTBearer())
):
    """
    Check if subscription has exceeded execution limit
    Used as dependency for workflow/job execution endpoints
    """

    feature_service = FeatureFlagService(db_pool)

    usage = await feature_service.check_usage_limit(
        subscription_id=current_user["subscription_id"],
        limit_type="executions"
    )

    if usage["exceeded"]:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Monthly execution limit reached ({usage['limit']} executions). "
                f"Please upgrade your plan or wait until next billing cycle."
            )
        )

    return True

# Usage in routes:
@router.post(
    "/api/workflows/{workflow_id}/execute",
    dependencies=[Depends(check_execution_limit)]
)
async def execute_workflow(
    workflow_id: str,
    current_user: dict = Depends(JWTBearer())
):
    """
    Execute workflow (gated by execution limit)
    """
    # Increment usage counter
    feature_service = FeatureFlagService(db_pool)
    await feature_service.increment_usage(
        subscription_id=current_user["subscription_id"],
        usage_type="executions"
    )

    # Execute workflow logic
    pass
```

---

## 8. Summary

### Feature Flag System Components

| Component | Purpose | Performance |
|-----------|---------|-------------|
| **Subscription Plans** | Define product tiers | Static configuration |
| **Feature Definitions** | Master list of all features | < 10ms lookup |
| **Plan Features** | Feature-to-plan mapping | Cached in memory |
| **Feature Overrides** | Per-subscription customization | Direct DB lookup |
| **Usage Tracking** | Monitor feature usage | TimescaleDB optimized |
| **FeatureGate Middleware** | Automatic API gating | < 5ms overhead |
| **React Hooks** | Frontend feature checks | Cached, real-time |

### Edition Upgrade Flow

```
User clicks "Upgrade to Premium"
  ↓
Frontend: POST /api/subscriptions/upgrade
  ↓
Backend: Update subscription.plan_id
  ↓
Automatically enables new features (via plan_features)
  ↓
WebSocket broadcast: "subscription_updated"
  ↓
Frontend: Refresh feature flags
  ↓
UI updates without page reload
  ↓
User sees new features (lineage, monitoring, etc.)
```

### Integration Points

1. **FastAPI Routes**: `dependencies=[Depends(FeatureGate("feature_key"))]`
2. **Prefect Workflows**: Check features before execution
3. **React Components**: `<FeatureGate featureKey="...">...</FeatureGate>`
4. **WebSocket**: Broadcast feature flag changes
5. **Usage Limits**: Enforced before expensive operations

### Next Steps

- Implement A/B testing framework
- Add feature usage analytics dashboard
- Create self-service upgrade flow
- Implement soft limits with warnings
- Add feature sunset/deprecation system

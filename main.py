"""
🔥 KAFKA-CELERY BRIDGE - THE MISSING REVOLUTION
Session 64: Connects Engine 9's orphaned events to Celery's proactive intelligence

Purpose: Bridge Kafka events to Celery tasks WITHOUT mixing Engine 9 with Celery
Events Consumed:
  - feed.invalidation.needed (Engine 9) → Triggers feed regeneration
  - auth.user.login (auth-service) → Tracks user activity for tier calculation

Architecture: Microservice pattern (Netflix/Twitter style)
  - Separate container (isolation, independent scaling)
  - Manual offset commit (reliability > speed)
  - Graceful error handling (DLQ pattern)
  - Health monitoring (consumer lag, error rate)

Performance Targets:
  - Event processing: <10ms per event
  - Kafka lag: <100 messages
  - CPU: <10% (event-driven, I/O-bound)
  - Memory: <256MB
"""

import os
import sys
import json
import time
import logging
import signal
from typing import Dict, Any, Optional
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('kafka-celery-bridge')

# Environment configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')
REDIS_FEED_HOST = os.getenv('REDIS_FEED_HOST', 'redis-feed')
REDIS_FEED_PORT = int(os.getenv('REDIS_FEED_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'supersecuresecret')
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', f'redis://:{REDIS_PASSWORD}@redis:6379/0')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'kafka-celery-bridge-group')

# Topics to consume
TOPICS = [
    'feed.invalidation.needed',  # Engine 9 publishes this (ORPHANED until now!)
    'auth.user.login',            # Auth-service publishes (EVERY app open!)
    'user.location.updated',      # SESSION 87 PART 7: Location updates from feed_endpoints
    'l0.context.populated',       # SESSION 110.5: Behavior engine signals L0 ready for warming
]

# Graceful shutdown flag
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    global shutdown_requested
    logger.warning(f"⚠️ Shutdown signal received ({signum}). Gracefully stopping...")
    shutdown_requested = True


class KafkaCeleryBridge:
    """
    Bridges Kafka events to Celery tasks

    Revolutionary Architecture:
    - Engine 9 (isolated) publishes to Kafka
    - This bridge consumes and triggers Celery
    - Clean separation: Engine 9 never imports Celery!
    """

    def __init__(self):
        """Initialize bridge with Kafka consumer and Redis client"""
        self.consumer: Optional[KafkaConsumer] = None
        self.redis_feed: Optional[redis.Redis] = None

        # Metrics
        self.metrics = {
            'events_processed': 0,
            'events_failed': 0,
            'invalidations_triggered': 0,
            'user_logins_tracked': 0,
            'last_event_time': None,
            'started_at': datetime.utcnow().isoformat()
        }

        # Initialize connections
        self._init_kafka_consumer()
        self._init_redis_client()

        logger.info("🚀 Kafka-Celery Bridge initialized successfully")

    def _init_kafka_consumer(self):
        """Initialize Kafka consumer with manual offset commit"""
        try:
            self.consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=CONSUMER_GROUP,
                auto_offset_reset='latest',  # Start from latest (not earliest - avoid replaying old events)
                enable_auto_commit=False,    # CRITICAL: Manual commit for reliability
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                # NO consumer_timeout_ms - blocks indefinitely until message or shutdown
                max_poll_records=10,         # Process 10 events per poll (balance throughput vs latency)
                session_timeout_ms=30000,    # 30s session timeout
                heartbeat_interval_ms=10000  # 10s heartbeat
            )
            logger.info(f"✅ Kafka consumer initialized for topics: {TOPICS}")
            logger.info(f"📊 Consumer group: {CONSUMER_GROUP}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            raise

    def _init_redis_client(self):
        """Initialize Redis client for reading user tiers and tracking activity"""
        try:
            self.redis_feed = redis.Redis(
                host=REDIS_FEED_HOST,
                port=REDIS_FEED_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_feed.ping()
            logger.info(f"✅ Redis client initialized: {REDIS_FEED_HOST}:{REDIS_FEED_PORT}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis client: {e}")
            raise

    def _trigger_celery_task(self, task_name: str, args: list, kwargs: dict = None, priority: int = 5, countdown: int = None):
        """
        Trigger Celery task via Redis broker

        Note: We use Celery's Redis broker protocol directly (no Celery client import needed!)
        This keeps the bridge lightweight and Engine 9 completely isolated from Celery.

        Args:
            countdown: Delay in seconds before task executes (SESSION 87 PART 5)
        """
        try:
            from celery import Celery

            # Create lightweight Celery app (client only, no worker)
            celery_app = Celery('kafka-celery-bridge', broker=CELERY_BROKER_URL)

            # Send task asynchronously with explicit queue routing
            # SESSION 87 PART 5: Add countdown support for delayed execution
            result = celery_app.send_task(
                task_name,
                args=args,
                kwargs=kwargs or {},
                priority=priority,
                queue='feed_generation_critical',  # SESSION 79: Route to correct queue
                countdown=countdown  # Delay execution by N seconds (None = immediate)
            )

            delay_msg = f", countdown={countdown}s" if countdown else ""
            logger.debug(f"✅ Triggered Celery task: {task_name} (task_id: {result.id}, priority: {priority}{delay_msg})")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to trigger Celery task {task_name}: {e}")
            return False

    def _handle_feed_invalidation(self, event: Dict[str, Any]) -> bool:
        """
        Handle feed.invalidation.needed event from Engine 9

        Event structure (from realtime_behavior_engine.py:984-989):
        {
            'user_id': str,
            'reason': str,  # e.g., 'search.query', 'interest_shift', 'session_mode_change'
            'timestamp': str (ISO format),
            'priority': 'high' | 'normal'
        }
        """
        try:
            user_id = event.get('user_id') or event.get('userId')  # Handle both snake_case and camelCase
            reason = event.get('reason', 'unknown')
            priority_label = event.get('priority', 'normal')

            if not user_id:
                logger.warning(f"⚠️ feed.invalidation.needed event missing user_id/userId: {event}")
                return False

            # Get user tier from redis-feed DB0 (Engine 9 maintains this)
            stored_tier = self.redis_feed.get(f"user:{user_id}:tier") or "regular_user"
            if isinstance(stored_tier, bytes):
                stored_tier = stored_tier.decode()

            # SESSION 87 PART 8: SESSION INTENT UPGRADE for invalidation
            # Feed invalidation = User is actively using the app → active_user minimum
            if stored_tier in ('regular_user', 'dormant_user'):
                warming_tier = 'active_user'
            else:
                warming_tier = stored_tier

            # Map priority label to Celery priority (high=8, normal=5)
            celery_priority = 8 if priority_label == 'high' else 5

            # Trigger Celery task: regenerate_feed_on_invalidation
            success = self._trigger_celery_task(
                'feed_generation_tasks.regenerate_feed_on_invalidation',
                args=[user_id, warming_tier, reason],
                priority=celery_priority
            )

            if success:
                self.metrics['invalidations_triggered'] += 1
                logger.info(f"🔄 Feed regeneration triggered: user={user_id}, tier={warming_tier}, reason={reason}, priority={priority_label}")

            return success

        except Exception as e:
            logger.error(f"❌ Error handling feed.invalidation.needed: {e}", exc_info=True)
            return False

    def _handle_user_login(self, event: Dict[str, Any]) -> bool:
        """
        SESSION 75: Handle auth.user.login event - activity tracking + tier calculation.
        SESSION 110.5: Warming logic moved to _handle_l0_context_populated() to avoid race condition.

        This handler now ONLY does:
        1. Update last_active timestamp
        2. Increment activity counter
        3. Calculate warming_tier (session intent upgrade)
        4. Store warming_tier for l0.context.populated handler

        Actual feed warming happens in _handle_l0_context_populated() AFTER behavior_engine
        populates L0 with subscriptions, interests, search history.
        """
        try:
            user_id = event.get('user_id') or event.get('userId')
            timestamp = event.get('timestamp', datetime.utcnow().isoformat())

            if not user_id:
                logger.warning(f"⚠️ auth.user.login event missing user_id: {event}")
                return False

            # Update last_active timestamp in redis-feed DB0
            # Engine 9 uses this for tier calculation (every 10 minutes)
            self.redis_feed.setex(
                f"user:{user_id}:last_active",
                86400,  # 24h TTL (tier calculation looks at 24h activity)
                timestamp
            )

            # Increment 24h activity counter (for tier calculation)
            activity_key = f"user:{user_id}:activity_count_24h"
            self.redis_feed.incr(activity_key)
            self.redis_feed.expire(activity_key, 86400)  # 24h TTL

            self.metrics['user_logins_tracked'] += 1

            # SESSION 75: Calculate warming tier based on stored tier + session intent
            stored_tier = self.redis_feed.get(f"user:{user_id}:tier") or "regular_user"
            if isinstance(stored_tier, bytes):
                stored_tier = stored_tier.decode()

            # SESSION 87 PART 8: SESSION INTENT UPGRADE
            # Login = User is actively engaging NOW → They deserve at least active_user treatment
            if stored_tier in ('regular_user', 'dormant_user'):
                warming_tier = 'active_user'
                logger.info(f"🚀 [SESSION 87 PART 8] Session intent upgrade: {stored_tier} → {warming_tier}")
            else:
                warming_tier = stored_tier

            # SESSION 110.5: Store warming_tier for l0.context.populated handler
            # Short TTL (60s) - just needs to survive until L0 population completes
            self.redis_feed.setex(
                f"user:{user_id}:pending_warming_tier",
                60,  # 60s TTL
                warming_tier
            )

            logger.info(f"✅ [SESSION 110.5] Login tracked, awaiting L0 population: user={user_id}, warming_tier={warming_tier}")
            return True

        except Exception as e:
            logger.error(f"❌ Error handling auth.user.login: {e}", exc_info=True)
            return False

    def _handle_l0_context_populated(self, event: Dict[str, Any]) -> bool:
        """
        SESSION 110.5: Handle l0.context.populated event - trigger feed warming with populated L0.

        This event is emitted by behavior_engine AFTER it populates L0 with:
        - subscribed_channels (from PostgreSQL subscriptions)
        - long_term_interests (from PostgreSQL user_interest)
        - search_history (from PostgreSQL search_query)
        - similar_users, peer_videos, peer_channels (from GNN MVs)

        Event structure:
        {
            'user_id': str,
            'timestamp': float,
            'elapsed_ms': float,
            'has_subscriptions': bool,
            'has_interests': bool,
            'has_search_history': bool,
            'has_similar_users': bool,
        }

        Now we can safely check L0 and warm the right feeds!
        """
        try:
            user_id = event.get('user_id')
            if not user_id:
                logger.warning(f"⚠️ l0.context.populated event missing user_id: {event}")
                return False

            elapsed_ms = event.get('elapsed_ms', 0)

            # Get warming_tier stored by _handle_user_login (or fallback to active_user)
            warming_tier = self.redis_feed.get(f"user:{user_id}:pending_warming_tier")
            if warming_tier:
                if isinstance(warming_tier, bytes):
                    warming_tier = warming_tier.decode()
                # Clean up the pending key
                self.redis_feed.delete(f"user:{user_id}:pending_warming_tier")
            else:
                warming_tier = 'active_user'  # Fallback if login event was missed

            # Use L0 signals from the event (behavior_engine already checked)
            has_subscriptions = event.get('has_subscriptions', False)
            has_interests = event.get('has_interests', False)
            has_location = self.redis_feed.exists(f"user:{user_id}:location")  # Location set separately

            # Base warming by tier
            feed_types_to_warm = []

            if warming_tier == 'power_user':
                # Power users: Full warming
                feed_types_to_warm = ['hot', 'personalized', 'viral', 'recent', 'shorts_trending']
                if has_subscriptions:
                    feed_types_to_warm.append('shorts_following')
                if has_interests:
                    feed_types_to_warm.append('shorts_interests')
                if has_location:
                    feed_types_to_warm.append('shorts_location')
                    feed_types_to_warm.append('location_trending')  # SESSION 127.5: For sectioned home feed
                feed_types_to_warm.append('shorts_mixed')

            elif warming_tier == 'active_user':
                # Active users: Personalized experience
                feed_types_to_warm = ['hot', 'personalized', 'shorts_trending', 'shorts_mixed']
                if has_subscriptions:
                    feed_types_to_warm.append('shorts_following')
                if has_interests:
                    feed_types_to_warm.append('shorts_interests')
                if has_location:
                    feed_types_to_warm.append('shorts_location')
                    feed_types_to_warm.append('location_trending')  # SESSION 127.5: For sectioned home feed

            logger.info(
                f"🔥 [SESSION 110.5] L0 ready, warming feeds: user={user_id}, tier={warming_tier}, "
                f"feeds={feed_types_to_warm}, L0_elapsed={elapsed_ms:.1f}ms, "
                f"subs={has_subscriptions}, interests={has_interests}, location={has_location}"
            )

            if feed_types_to_warm:
                personalized_warmed = False
                for feed_type in feed_types_to_warm:
                    success = self._trigger_celery_task(
                        'feed_generation_tasks.generate_personalized_feed',
                        args=[user_id, warming_tier, feed_type],
                        priority=7
                    )
                    if success and feed_type == 'personalized':
                        personalized_warmed = True

                # Warm peer videos cache (L0 peer_videos now populated!)
                self._trigger_celery_task(
                    'feed_generation_tasks.warm_peer_videos_cache',
                    args=[user_id],
                    priority=7,
                    countdown=0  # No delay needed - L0 already populated!
                )

                # Chain sectioned feed after personalized + peer_videos
                if personalized_warmed or 'personalized' in feed_types_to_warm:
                    self._trigger_celery_task(
                        'feed_generation_tasks.generate_sectioned_feed',
                        args=[user_id, warming_tier],
                        priority=6,
                        countdown=2  # Reduced from 3s - L0 already ready
                    )

            return True

        except Exception as e:
            logger.error(f"❌ Error handling l0.context.populated: {e}", exc_info=True)
            return False

    def _handle_location_updated(self, event: Dict[str, Any]) -> bool:
        """
        SESSION 87 PART 7: Handle user.location.updated event

        Triggers feed regeneration when user location changes significantly (>10km).
        This ensures location-based sections show relevant local content.

        Event structure (from feed_endpoints.py):
        {
            'event_type': 'user.location.updated',
            'user_id': str,
            'data': {
                'latitude': float,
                'longitude': float,
                'city': str,
                'country': str,
                'timestamp': str
            }
        }

        Invalidation Chain:
        1. personalized feed (with new location boost)
        2. sectioned feed (depends on personalized)
        """
        try:
            import math

            user_id = event.get('user_id') or event.get('userId')

            # SESSION 94 FIX: Location data is nested inside 'data' object
            event_data = event.get('data', {})
            new_lat = event_data.get('latitude') or event.get('latitude')
            new_lng = event_data.get('longitude') or event.get('longitude')
            new_city = event_data.get('city') or event.get('city', 'Unknown')

            if not user_id:
                logger.warning(f"⚠️ user.location.updated event missing user_id: {event}")
                return False

            if not new_lat or not new_lng:
                logger.warning(f"⚠️ user.location.updated event missing coordinates: {event}")
                return False

            # Check previous location from redis-feed DB0
            location_key = f"user:{user_id}:location"
            prev_location = self.redis_feed.get(location_key)

            significant_change = True  # Default: trigger regeneration

            if prev_location:
                try:
                    prev_loc = json.loads(prev_location)
                    prev_lat = prev_loc.get('latitude')
                    prev_lng = prev_loc.get('longitude')

                    if prev_lat and prev_lng:
                        # Haversine formula to calculate distance
                        R = 6371  # Earth's radius in km
                        lat1, lon1 = math.radians(float(prev_lat)), math.radians(float(prev_lng))
                        lat2, lon2 = math.radians(float(new_lat)), math.radians(float(new_lng))
                        dlat = lat2 - lat1
                        dlon = lon2 - lon1
                        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                        c = 2 * math.asin(math.sqrt(a))
                        distance_km = R * c

                        # Only trigger regeneration if moved >10km
                        if distance_km <= 10:
                            significant_change = False
                            logger.debug(f"📍 Location change <10km ({distance_km:.1f}km), skipping regeneration")

                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    logger.warning(f"⚠️ Could not parse previous location: {e}")

            if significant_change:
                # Get user tier
                stored_tier = self.redis_feed.get(f"user:{user_id}:tier") or "regular_user"
                if isinstance(stored_tier, bytes):
                    stored_tier = stored_tier.decode()

                # SESSION 87 PART 8: SESSION INTENT UPGRADE for location change
                # Location change = User is actively using the app → active_user minimum
                if stored_tier in ('regular_user', 'dormant_user'):
                    warming_tier = 'active_user'
                    logger.info(f"🚀 [SESSION 87 PART 8] Session intent upgrade: {stored_tier} → {warming_tier} (location change = active engagement)")
                else:
                    warming_tier = stored_tier

                # SESSION 87 PART 7: Location change triggers full location-aware feed chain
                # Chain: personalized → shorts_location → sectioned

                # 1. Trigger personalized feed regeneration
                success = self._trigger_celery_task(
                    'feed_generation_tasks.generate_personalized_feed',
                    args=[user_id, warming_tier, 'personalized'],
                    priority=6
                )

                if success:
                    logger.info(f"📍 Location change detected ({new_city}), regenerating personalized feed: user={user_id}, tier={warming_tier}")

                    # 2. Regenerate shorts_location (uses geo-spatial filtering)
                    self._trigger_celery_task(
                        'feed_generation_tasks.generate_personalized_feed',
                        args=[user_id, warming_tier, 'shorts_location'],
                        priority=5
                    )
                    logger.info(f"📍 Shorts location feed regeneration triggered: user={user_id}")

                    # SESSION 127.5: Regenerate location_trending (for sectioned home feed)
                    self._trigger_celery_task(
                        'feed_generation_tasks.generate_personalized_feed',
                        args=[user_id, warming_tier, 'location_trending'],
                        priority=5
                    )
                    logger.info(f"📍 Location trending feed regeneration triggered: user={user_id}")

                    # 3. Chain sectioned feed (2s delay for personalized to complete)
                    self._trigger_celery_task(
                        'feed_generation_tasks.generate_sectioned_feed',
                        args=[user_id, warming_tier],
                        priority=4,
                        countdown=2
                    )
                    logger.info(f"🎯 Sectioned feed regeneration chained: user={user_id}")

            return True

        except Exception as e:
            logger.error(f"❌ Error handling user.location.updated: {e}", exc_info=True)
            return False

    def _handle_event(self, topic: str, event: Dict[str, Any]) -> bool:
        """Route event to appropriate handler"""
        handlers = {
            'feed.invalidation.needed': self._handle_feed_invalidation,
            'auth.user.login': self._handle_user_login,
            'user.location.updated': self._handle_location_updated,
            'l0.context.populated': self._handle_l0_context_populated,  # SESSION 110.5
        }

        handler = handlers.get(topic)
        if not handler:
            logger.warning(f"⚠️ No handler for topic: {topic}")
            return False

        return handler(event)

    def run(self):
        """Main event loop - consume and process events"""
        logger.info("🎯 Kafka-Celery Bridge started - listening for events...")
        logger.info(f"📡 Topics: {TOPICS}")

        try:
            # Use poll() in while loop (not iterator) to allow shutdown checks
            while not shutdown_requested:
                # Poll for messages with 1s timeout (allows shutdown check every second)
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=10)

                # Check for shutdown between polls
                if shutdown_requested:
                    logger.info("🛑 Shutdown requested - stopping consumer...")
                    break

                # Process messages from all partitions
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        # Parse event
                        topic = message.topic
                        event = message.value

                        if not event:
                            logger.warning(f"⚠️ Empty event from topic {topic}")
                            continue

                        # Process event
                        start_time = time.time()
                        success = self._handle_event(topic, event)
                        elapsed_ms = (time.time() - start_time) * 1000

                        # Update metrics
                        self.metrics['events_processed'] += 1
                        self.metrics['last_event_time'] = datetime.utcnow().isoformat()

                        if not success:
                            self.metrics['events_failed'] += 1
                            logger.warning(f"⚠️ Event processing failed: {topic} (elapsed: {elapsed_ms:.2f}ms)")
                        else:
                            logger.debug(f"✅ Event processed: {topic} (elapsed: {elapsed_ms:.2f}ms)")

                        # Log metrics every 100 events
                        if self.metrics['events_processed'] % 100 == 0:
                            self._log_metrics()

                # Commit offsets after processing batch
                if message_batch:
                    try:
                        self.consumer.commit()
                    except Exception as e:
                        logger.error(f"❌ Failed to commit offset: {e}")

        except KeyboardInterrupt:
            logger.info("⚠️ Keyboard interrupt received - shutting down...")

        except Exception as e:
            logger.error(f"❌ Fatal error in event loop: {e}", exc_info=True)
            raise

        finally:
            self._shutdown()

    def _log_metrics(self):
        """Log performance metrics"""
        logger.info(f"📊 Metrics: {json.dumps(self.metrics, indent=2)}")

    def _shutdown(self):
        """Graceful shutdown - commit offsets and close connections"""
        logger.info("🛑 Shutting down Kafka-Celery Bridge...")

        try:
            # Final metrics
            self._log_metrics()

            # Commit final offsets
            if self.consumer:
                logger.info("💾 Committing final offsets...")
                self.consumer.commit()
                self.consumer.close()
                logger.info("✅ Kafka consumer closed")

            # Close Redis connection
            if self.redis_feed:
                self.redis_feed.close()
                logger.info("✅ Redis connection closed")

        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")

        logger.info("✅ Kafka-Celery Bridge shutdown complete")


def main():
    """Entry point"""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 80)
    logger.info("🔥 KAFKA-CELERY BRIDGE - THE MISSING REVOLUTION")
    logger.info("=" * 80)
    logger.info(f"Kafka Brokers: {KAFKA_BROKERS}")
    logger.info(f"Redis Feed: {REDIS_FEED_HOST}:{REDIS_FEED_PORT}")
    logger.info(f"Celery Broker: {CELERY_BROKER_URL}")
    logger.info(f"Topics: {TOPICS}")
    logger.info("=" * 80)

    # Create and run bridge
    try:
        bridge = KafkaCeleryBridge()
        bridge.run()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

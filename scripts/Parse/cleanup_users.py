import argparse
import Parse
from datetime import timedelta


def cleanup_users(app_id, master_key, age):
    threshold = Parse.utc_datetime.UtcDateTime.now()
    threshold.datetime -= timedelta(days=age)
    now = Parse.utc_datetime.UtcDateTime.now()
    num_deleted = 0
    # Query all users
    conn = Parse.connection.Connection(app_id=app_id, master_key=master_key)
    query = Parse.query.Query()
    query.limit = 1000
    query.add('updatedAt', Parse.query.SelectorLessThan(threshold))
    user_list = Parse.query.Query.users(query, conn)[0]
    for user in user_list:
        username = user['username']
        # Don't touch monitor
        if username == 'monitor':
            continue

        # Verify that the user has not been updated for a while
        last_updated = Parse.utc_datetime.UtcDateTime(user['updatedAt'])
        last_updated.datetime += timedelta(days=age)
        if last_updated > now:
            print 'Skipping user %s (updated at %s)' % (username, user['updatedAt'])
            continue

        # Verify that a user has no more events
        query = Parse.query.Query()
        query.limit = 0
        query.count = 1
        query.add('client', Parse.query.SelectorEqual(username))
        conn = Parse.connection.Connection(app_id=app_id, master_key=master_key)
        event_count = Parse.query.Query.objects('Events', query, conn)[1]
        if event_count > 0:
            print 'Skipping user %s (%d events, %s)' % (username, event_count, user['updatedAt'])
            continue

        # Delete it
        print 'Deleting user %s' % user['username']
        user.delete(conn)
        num_deleted += 1

    print '%d users deleted' % num_deleted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app-id', help='Parse app ID')
    parser.add_argument('--master-key', help='master key')
    parser.add_argument('--age', type=int, help='number of days inactivity before the user is deleted')

    options = parser.parse_args()

    cleanup_users(app_id=options.app_id,
                  master_key=options.master_key,
                  age=options.age)

if __name__ == '__main__':
    main()
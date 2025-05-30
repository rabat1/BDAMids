if final_records:
    try:
        collection.delete_many({})  # Delete previous records
        collection.insert_many(final_records)  # Insert new records
        print("✅ Data successfully stored in MongoDB!")
    except Exception as e:
        print(f"⚠️ Error inserting into MongoDB: {e}")
else:
    print("⚠️ No valid records to store in MongoDB!")

use crate::storage::{Item, ItemStorage};
use rand::seq::SliceRandom;
use uuid::Uuid;

mod wrap;
pub use self::wrap::TemporaryFileWrap;

fn create_item() -> Item {
    Item {
        id: Uuid::new_v4(),
        part: "default".to_string(),
        views: 1,
        meta: Default::default(),
    }
}

#[test]
fn it_loads() {
    let storage = TemporaryFileWrap::load();
    std::mem::drop(storage);
}

#[test]
fn it_stores_an_item() {
    let storage = TemporaryFileWrap::load();
    let item = create_item();
    storage.items_insert(&item).expect("could not insert item");
    let loaded = storage
        .find_item(&item.part, item.id)
        .expect("could not load item")
        .expect("item not found");
    assert_eq!(item, loaded);
}

#[test]
fn it_handles_near_requests() {
    let storage = TemporaryFileWrap::load();
    let item = create_item();
    let near = create_item();
    storage.items_insert(&item).expect("could not insert item");
    storage
        .items_add_near(&item.part, item.id, near.id)
        .expect("could not add near");
    let list = storage
        .find_items_near(&item.part, item.id)
        .expect("could not retrieve item list");

    assert_eq!(list.nmods, 1);
    assert_eq!(list.epoch, 0);
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0], (near.id, 1.0));
}

#[test]
fn it_calculates_near_items() {
    let storage = TemporaryFileWrap::load();
    let base = create_item();
    let items = (0..64).map(|_| create_item()).collect::<Vec<_>>();
    storage.items_insert(&base).expect("could not insert item");
    for item in items.iter() {
        storage.items_insert(item).expect("could not insert item");
    }
    let mut insertions = items
        .iter()
        .enumerate()
        .flat_map(|(i, item)| {
            let c = 65 - i;
            (0..c).map(move |_| item)
        })
        .collect::<Vec<_>>();

    insertions.shuffle(&mut rand::thread_rng());
    let ids = insertions.iter().map(|item| item.id);
    storage
        .items_add_bulk_near(&base.part, std::iter::once((base.id, ids)))
        .expect("could not add near");

    let list = storage
        .find_items_near(&base.part, base.id)
        .expect("could not load near list");

    assert_eq!(list.nmods, 0);
    assert_eq!(list.epoch, 0);
    assert!(list.items.len() > 63);
    // Since this one appears the most often, it should both be at the top of
    // the list _and_ have the highest value.
    assert_eq!(list.items[0].0, items[0].id);
}

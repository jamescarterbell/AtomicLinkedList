use std::sync::atomic::{Ordering, AtomicPtr, AtomicBool};
use std::sync::{Mutex, Arc};
use atomic_markable_ptr::AtomicMarkablePtr;
use rand::{RngCore, prelude::SliceRandom};

fn main(){
    let mut time = 0;
    // Create the empty list
    for i in 0..25{
        let head = std::sync::Arc::new(OrderedList::new());

        let start = std::time::Instant::now();
        // Have 4 workers
        let n_threads = 4;

        // We're just going to preseperate the unsorted bag so I don't need to write a wait free deque thingy
        let mut n_vecs: Vec<Vec<usize>> = std::iter::repeat_with(|| Vec::new()).take(n_threads).collect();

        // This randomly chooses which worker gets which number
        let mut rng = rand::thread_rng();
        for i in 0..1000000{             
            let thread = (rng.next_u32() as usize) % n_threads;
            n_vecs[thread].push(i);
        }

        // This unorders the bags the workers get
        for vec in n_vecs.iter_mut(){
            vec.shuffle(&mut rng);
        }

        let mut n_handles = Vec::new();

        let vecs: Arc<Vec<Mutex<Vec<usize>>>> = std::sync::Arc::new(n_vecs.drain(..).map(|v| Mutex::new(v)).collect());

        // Have each worker randomly add elements from their bag to the chain
        // or remove elements they've added to the chain from the chain.
        // They could in theory remove any number, but I'm only letting workers remove numbers they know are valid.
        for thread in 0..n_threads{
            let head_clone = head.clone();
            let vec_clone = vecs.clone();

            n_handles.push(std::thread::spawn(move ||{
                let mut created_vec =  Vec::new();
                let mut bag_vec = vec_clone[thread].lock().unwrap();
                let mut l_rng = rand::thread_rng();
                let mut complete_vec = Vec::new();
                while bag_vec.len() > 0 || created_vec.len() > 0{
                    if l_rng.next_u32() % 2 == 0{
                        if bag_vec.len() > 0{
                            let gift = bag_vec.pop().unwrap();
                            head_clone.add(gift);
                            created_vec.push(gift);
                        }
                    }
                    else{
                        if created_vec.len() > 0{
                            let gift = created_vec.pop().unwrap();
                            head_clone.delete(&gift);
                            complete_vec.push(gift);
                        }
                    }
                }
                complete_vec
            }));
        }

        let mut done_gifts = Vec::new();
        for thread in n_handles.drain(..){
            done_gifts.append(&mut thread.join().unwrap());
        }
        time += start.elapsed().as_millis();

        done_gifts.sort();

        // Make sure the chain is empty
        for (i, val) in head.iter().enumerate(){
            //println!("{}, {}", i, val);
            assert_eq!(i, *val);
        }

        // Make sure all the gifts are signed
        for (i, val) in done_gifts.iter().enumerate(){
            //println!("{}, {}", i, val);
            assert_eq!(i, *val);
        }
    }

    println!("{}", time/25);
}

pub enum FindResult<T>{
    /// The find one of two things:
    /// The head was empty, or you're looking for something smaller than head.
    /// Either way, you'll be CASing self.head, with target as the current value.
    Head{target: *mut OrderedListNode<T>},
    /// You've found what you're looking for, or the first item bigger than it, at target.
    /// This means you'll be CASing prev.nxt, with target as current.
    Found{prev: *mut OrderedListNode<T>, target: *mut OrderedListNode<T>},
    /// What you're looking for is bigger than everything else in the list.
    /// This means you'll be CASing prev.nxt, null_mut as current.
    NotFound{prev: *mut OrderedListNode<T>}
}

pub struct OrderedList<T>
    where T: Eq + PartialEq + PartialOrd + Ord + Copy{
    head: AtomicMarkablePtr<OrderedListNode<T>>,
}

impl<T> OrderedList<T>
    where T: Eq + PartialEq + PartialOrd + Ord + std::fmt::Display + Copy{
    fn new() -> Self{
        OrderedList{
            head: AtomicMarkablePtr::new_raw(std::ptr::null_mut()),
        }
    }
    
    /// Add a value to the list, this operation cannot fail
    pub fn add(&self, value: T){

        let node = Box::into_raw(Box::new(OrderedListNode::new(value)));
        loop{
            let prev_node;
            let (prev, target) = match self.find(&value){
                FindResult::Head { target } => {(&self.head, target)}
                FindResult::Found { prev, target } => {
                    prev_node = unsafe{prev.as_ref().unwrap()};
                    (&prev_node.nxt, target)
                }
                FindResult::NotFound { prev } => {
                    prev_node = unsafe{prev.as_ref().unwrap()};
                    (&prev_node.nxt, std::ptr::null_mut())
                }
            };

            unsafe{(*node).nxt.store(target, false, Ordering::SeqCst)};
            // We should currently be looking at target and not be marked for deletion
            // We should end up looking at the node, and also not marked for deletion
            if prev.compare_and_swap(target, false, node, false, Ordering::SeqCst) == (target, false){
                return;
            }
        }
    }

    /// Returns a ref to the value before the one being looked for, and the next pointer that points to the value you're looking for
    fn find<'d, 'r>(&self, target: &'d T) -> FindResult<T>{
       let inter =  self.head.load(Ordering::SeqCst);
       let mut looking_at = inter.0;
       let mut standing_on: (*mut OrderedListNode<T>, bool) = (std::ptr::null_mut(), inter.1);
       let mut previous: *mut OrderedListNode<T> = std::ptr::null_mut();
       loop{
           match standing_on{
                // If we're not standing on anything yet, 
                // we will never have something marked for deletion
                // so this would be an error
                (ptr, true) if ptr == std::ptr::null_mut()  => panic!("Head pointer is marked, this can't happne"),

                // This will occur if we haven't stepped into the list yet
                (ptr, false) if ptr == std::ptr::null_mut() => {
                    // This will occur if the head is non existent
                    if looking_at == std::ptr::null_mut(){
                        return FindResult::Head{target: ptr}
                    }
                    // This will occur if the head does exist, we just need to make sure it's not bigger than what we're looking for
                    else{
                        match looking_at{
                            // We're at the end of the list!
                            look if look == std::ptr::null_mut() => return FindResult::NotFound{prev: ptr},
                            // We're in the list, but looking at something bigger or equal to our target
                            look if unsafe{(*look).data} >= *target =>{
                                return FindResult::Head{target: look};
                            },
                            // We're in the list, but we haven't found our target yet
                            look =>{
                                previous = standing_on.0;
                                let inter =  unsafe{(*look).nxt.load(Ordering::SeqCst)};
                                standing_on = (looking_at, inter.1);
                                looking_at = inter.0;
                            }
                        }
                    }
                },

                // We're standing on a node in the list, and this node is marked for deletion
                (ptr, true) =>{
                    // If the previous node we were standing on is null, we will be swapping out head
                    // for whatever we're looking at.
                    if previous == std::ptr::null_mut(){
                        if self.head.compare_and_swap(ptr, false, looking_at, false, Ordering::SeqCst) == (ptr, false){
                            //drop(unsafe{Box::from_raw(ptr)});
                        }
                        
                        // Just restart the search
                        let inter =  self.head.load(Ordering::SeqCst);
                        looking_at = inter.0;
                        standing_on = (std::ptr::null_mut(), inter.1);
                        previous= std::ptr::null_mut();
                    }
                    // Otherwise, we must do a mid list deletiong
                    else{
                        // Previous should be looking at current
                        if unsafe{(*previous).nxt.compare_and_swap(ptr, false, looking_at, false, Ordering::SeqCst)} == (ptr, false){
                            //drop(unsafe{Box::from_raw(ptr)});
                        }

                        // Just restart the search
                        let inter =  self.head.load(Ordering::SeqCst);
                        looking_at = inter.0;
                        standing_on = (std::ptr::null_mut(), inter.1);
                        previous= std::ptr::null_mut();
                    }
                },

                // We're standing on a node in the list, and it's not marked for deletion
                (ptr, false) =>{
                    match looking_at{
                        // We're at the end of the list!
                        look if look == std::ptr::null_mut() => return FindResult::NotFound{prev: ptr},
                        // We're in the list, but looking at something bigger or equal to our target
                        look if unsafe{(*look).data} >= *target =>{
                            return FindResult::Found{prev: ptr, target: look};
                        },
                        // We're in the list, but we haven't found our target yet
                        look =>{
                            previous = standing_on.0;
                            let inter =  unsafe{(*look).nxt.load(Ordering::SeqCst)};
                            standing_on = (looking_at, inter.1);
                            looking_at = inter.0;
                        }
                    }
                }
           }
       }
    }

    /// Attempts to delete the node at max_value
    /// This will return Ok if it can successfully logically delete
    /// otherwise it will fail, meaning the value was not in the list
    pub fn delete(&self, value: &T) -> Result<(), ()>{
        loop{
            let target = match self.find(&value){
                FindResult::Head { target } => {target}
                FindResult::Found { prev: _, target } => {target}
                FindResult::NotFound { prev: _ } => {return Err(())}
            };

            if unsafe{(*target).data} == *value{
                let target_swap = unsafe{(*target).nxt.load(Ordering::SeqCst)};
                if unsafe{(*target).nxt.compare_and_swap(target_swap.0, target_swap.1, target_swap.0, true, Ordering::SeqCst)} == (target_swap.0, true){
                    return Ok(());
                }
                continue;
            }
            return Err(());
        }
    }

    // Find the node, check it's mark if you find it
    pub fn contains(&self, value: &T) -> bool{
        let target = match self.find(value){
            FindResult::Head{ target: t } => t,
            FindResult::Found{ prev: _, target: t } => t,
            FindResult::NotFound{ prev: _ } => return false,
        };

        unsafe{!(*target).nxt.mark(Ordering::SeqCst)}
    }

    pub fn iter(&self) -> OrderedAtomicListIterator<T>{
        OrderedAtomicListIterator{
            current: unsafe{self.head.load(Ordering::SeqCst).0.as_ref()}
        }
    }
}

// This is just a container
pub struct OrderedListNode<T>{
    data: T,
    nxt: AtomicMarkablePtr<OrderedListNode<T>>,
}

impl<T> OrderedListNode<T>{
    pub fn new(value: T) -> Self{
        Self{
            data: value,
            nxt: AtomicMarkablePtr::new(std::ptr::null_mut(), false),
        }
    }
}

pub struct OrderedAtomicListIterator<'n, T>
    where T: Eq + PartialEq + PartialOrd + Ord{
    current: Option<&'n OrderedListNode<T>>,
}

impl<'n, T> Iterator for OrderedAtomicListIterator<'n, T>
    where T: Eq + PartialEq + PartialOrd + Ord{

    type Item = &'n T;

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        loop{
            match self.current{
                Some(node) =>{
                    let node_info =  node.nxt.load(Ordering::SeqCst);
                    let node_deleted = node_info.1;
                    let ret_val = Some(&node.data);
                    self.current = match node_info.0 == std::ptr::null_mut(){
                        true => None,
                        false => unsafe{Some(&*node.nxt.load(Ordering::SeqCst).0)}
                    };
                    if node_deleted{
                        continue;
                    }
                    return ret_val
                },
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests{

    use crate::{OrderedList};

    #[test]
    fn insert_first_test_one_thread(){
        let head = OrderedList::new();
        println!("Adding 1");
        head.add(1);
        println!("Adding 4");
        head.add(4);
        println!("Adding 0");
        head.add(0);
        println!("Adding 2");
        head.add(2);
        println!("Adding 5");
        head.add(5);
        println!("Delete 5");
        assert_eq!(Ok(()), head.delete(&5));
        println!("Delete 2");
        assert_eq!(Ok(()), head.delete(&2));
        head.add(3);

        assert_eq!(head.contains(&5), false);

        let other = &[0, 1, 3, 4];
        for (val, val2) in head.iter().zip(other.iter()){
            println!("{}", *val);
            assert_eq!(*val, *val2);
        }
    }

    use rand::{RngCore, prelude::SliceRandom};

    
    #[test]
    fn insertion_test_multiple_threads(){
        for q in 0..1000{
            
            let head = std::sync::Arc::new(OrderedList::new());
            let n_threads = 10;

            let mut n_vecs: Vec<Vec<usize>> = std::iter::repeat_with(|| Vec::new()).take(n_threads).collect();

            let mut rng = rand::thread_rng();
            for i in 0..10000{             
                let thread = (rng.next_u32() as usize) % n_threads;
                n_vecs[thread].push(i);
            }

            for vec in n_vecs.iter_mut(){
                vec.shuffle(&mut rng);
            }

            let mut n_handles = Vec::new();

            let vecs = std::sync::Arc::new(n_vecs);

            for thread in 0..n_threads{
                let head_clone = head.clone();
                let vec_clone = vecs.clone();

                n_handles.push(std::thread::spawn(move ||{
                    for i in vec_clone[thread].iter(){
                        head_clone.add(*i);
                    }
                }))
            }

            for thread in n_handles.drain(..){
                thread.join();
            }

            for (i, val) in head.iter().enumerate(){
                assert_eq!(i, *val);
            }
        }
    }

    #[test]
    fn insertion_deletion_test_multiple_threads(){
        for q in 0..100{
            
            let head = std::sync::Arc::new(OrderedList::new());
            let n_threads = 10;

            let mut n_vecs: Vec<Vec<usize>> = std::iter::repeat_with(|| Vec::new()).take(n_threads).collect();

            let mut rng = rand::thread_rng();
            for i in 0..10000{             
                let thread = (rng.next_u32() as usize) % n_threads;
                n_vecs[thread].push(i);
            }

            for vec in n_vecs.iter_mut(){
                vec.shuffle(&mut rng);
            }

            let mut n_handles = Vec::new();

            let vecs = std::sync::Arc::new(n_vecs);

            for thread in 0..n_threads{
                let head_clone = head.clone();
                let vec_clone = vecs.clone();


                n_handles.push(std::thread::spawn(move ||{
                    let mut created_vec =  Vec::new();
                    for i in vec_clone[thread].iter(){
                        head_clone.add(*i);
                        created_vec.push(*i);
                    }
                    
                    for i in created_vec.drain(..){
                        head_clone.delete(&i);
                    }
                }));
            }

            for thread in n_handles.drain(..){
                thread.join();
            }

            for (i, val) in head.iter().enumerate(){
                println!("{}, {}", i, val);
                assert_eq!(i, *val);
            }
        }
    }
}
pragma solidity >=0.6.6;

contract EventsTestContract {

    uint32 counter_foo;
    uint32 counter_bar;

    event Constructed(address indexed by, address data);
    event Foo(address indexed by, uint32 indexed num);
    event Bar(address indexed by, uint32 indexed num);

    constructor() public {
        emit Constructed(msg.sender, msg.sender);
    }

    function foo() public {
        counter_foo += 1;
        emit Foo(msg.sender, counter_foo);
    }

    function bar() public {
        emit Bar(msg.sender, counter_bar);
        counter_bar += 1;

        emit Bar(msg.sender, counter_bar);
        counter_bar += 1;
    }

    function get_foo() public view returns (uint32) {
        return counter_foo;
    }

    function delete_foo() public returns (uint32) {
        delete counter_foo;
        return counter_foo;
    }
}

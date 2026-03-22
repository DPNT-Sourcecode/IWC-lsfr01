
class HelloSolution:
    
    # friend_name = unicode string
    def hello(self, friend_name: str) -> str:
        """
        Return a string in the format "Hello, {friend_name}!" where {friend_name} is replaced with the value of the friend_name parameter.
        :param friend_name: The name of the friend to greet.
        :return: A greeting string.
        """
        if not isinstance(friend_name, str):
            raise ValueError("friend_name must be a string")
        
        return f"Hello, {friend_name}!"

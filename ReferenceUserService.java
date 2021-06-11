package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.User;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceUserService implements UserService {
    @Override
    public void removeUser(int userId) {
        try {int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from user1 where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,userId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                PreparedStatement stmt = connection.prepareStatement("call remove_user(?)");
                stmt.setInt(1, userId);
                stmt.execute();
                connection.commit();
                connection.close();
            }
            else{
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        ResultSet rst=null;
        List<User> ulist=new ArrayList<>();
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            String sql = "select * from user1 ";
            PreparedStatement pst = connection.prepareStatement(sql);

            ResultSet resultSet =  pst.executeQuery();
            while(resultSet.next()){
                int id=resultSet.getInt(1);
                String firstname=resultSet.getString(2);
                String lastname=resultSet.getString(3);
                int role=resultSet.getInt(4);
                User u=new User() {
                };
                u.id=id;
                String fullname;
                if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
                    fullname = firstname + " " + lastname;
                else
                    fullname = firstname + lastname;
                u.fullName=fullname;
                ulist.add(u);
            }
            resultSet.close();
            pst.close();
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e){
            e.printStackTrace();
        }
        List a=(List)rst;
        return ulist;
    }

    @Override
    public User getUser(int userId) {

        ResultSet rst=null;
        User result=null;
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from user1 where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,userId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

             PreparedStatement stmt = connection.prepareStatement("get_User(?)");
            stmt.setInt(1,userId);
            stmt.executeUpdate();
            rst = stmt.getGeneratedKeys();
            rst=stmt.executeQuery("select * from user1 where id="+userId);
            result.id=(int)rst.getObject(1);
            StringBuilder a=new StringBuilder();
            a.append((String)rst.getObject(2));
            a.append((String)rst.getObject(3));
            result.fullName=a.toString();
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e){
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;
        return result;
    }
}
